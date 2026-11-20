#!/usr/bin/env python3
"""
Deadlock Handling and Banker's Algorithm  (OS lab, 5 marks)

Parts
  threads : controlled circular wait with Python threads, Coffman conditions,
            deadlock detection (wait-for graph watchdog), recovery (rollback +
            back-off) and prevention (global resource ordering).
  banker  : Need matrix + Banker's safety algorithm + resource-request algorithm.
  detect  : deadlock-detection algorithm on Allocation/Request matrices and
            controlled recovery by process termination.

Usage
  python deadlock_bankers.py                 # run everything with built-in data
  python deadlock_bankers.py --part threads
  python deadlock_bankers.py --part banker --interactive   # type your own matrices
All runs terminate: every blocking acquire in the thread demos has a timeout.
"""
import argparse
import threading
import time

# --------------------------------------------------------------------------
# Logging helper (a Lock keeps lines from different threads from interleaving)
# --------------------------------------------------------------------------
_LOG_LOCK = threading.Lock()
_T0 = time.perf_counter()


def reset_clock():
    global _T0
    _T0 = time.perf_counter()


def log(msg):
    with _LOG_LOCK:
        print(f"[{time.perf_counter() - _T0:5.2f}s] {msg}", flush=True)


def banner(title):
    print("\n" + "=" * 72 + f"\n{title}\n" + "=" * 72)


# --------------------------------------------------------------------------
# PART 1: threads, circular wait, detection, recovery, prevention
# --------------------------------------------------------------------------
class Tracker:
    """Records who holds / waits for each resource so a watchdog can build the
    wait-for graph and find a cycle (= deadlock)."""

    def __init__(self):
        self._m = threading.Lock()
        self.holder = {}   # resource name -> thread name
        self.waiting = {}  # thread name   -> resource name

    def set_waiting(self, thread, res):
        with self._m:
            self.waiting[thread] = res

    def grant(self, res, thread):
        with self._m:
            self.waiting.pop(thread, None)
            self.holder[res] = thread

    def give_up(self, thread):
        with self._m:
            self.waiting.pop(thread, None)

    def drop(self, res):
        with self._m:
            self.holder.pop(res, None)

    def snapshot(self):
        with self._m:
            return dict(self.holder), dict(self.waiting)

    def find_cycle(self):
        """Return [(thread, resource_it_waits_for, holder_of_that_resource)] or None."""
        with self._m:
            for start in list(self.waiting):
                path, cur = [start], start
                while True:
                    res = self.waiting.get(cur)
                    if res is None:
                        break
                    nxt = self.holder.get(res)
                    if nxt is None:
                        break
                    if nxt in path:
                        cyc = path[path.index(nxt):]
                        return [(t, self.waiting[t], self.holder[self.waiting[t]]) for t in cyc]
                    path.append(nxt)
                    cur = nxt
        return None


class TrackedLock:
    """A mutual-exclusion resource (threading.Lock) that reports to the Tracker."""

    def __init__(self, name, rank, tracker):
        self.name, self.rank, self.tracker = name, rank, tracker
        self._lock = threading.Lock()

    def acquire(self, owner, timeout):
        self.tracker.set_waiting(owner, self.name)
        ok = self._lock.acquire(timeout=timeout)
        if ok:
            self.tracker.grant(self.name, owner)
        else:
            self.tracker.give_up(owner)
        return ok

    def release(self):
        self.tracker.drop(self.name)
        self._lock.release()


def explain_coffman(cycle, tracker):
    holder, waiting = tracker.snapshot()
    print("    Coffman conditions observed in this deadlock:")
    print(f"      1. Mutual exclusion : each resource is a Lock with one holder -> {holder}")
    t, r, h = cycle[0]
    held_by_t = [x for x, o in holder.items() if o == t]
    print(f"      2. Hold and wait    : {t} holds {held_by_t} while waiting for {r}")
    print("      3. No preemption    : a Lock cannot be taken away; only its holder can release it")
    chain = " -> ".join(f"{t} waits {r} (held by {h})" for t, r, h in cycle)
    print(f"      4. Circular wait    : {chain}")


def run_scenario(mode, rounds=3, timeout=1.0):
    """
    mode = 'naive'   : T1 takes A then B, T2 takes B then A. No recovery.
                       Circular wait is forced; a timeout prevents a permanent block.
           'recover' : same workload, but on timeout the thread rolls back
                       (releases what it holds), backs off, and retries.
           'ordered' : same workload, but every thread acquires resources in the
                       global order R_A < R_B  (breaks circular wait = prevention).
    Workload (identical in all modes): each of T1/T2 must complete `rounds` rounds;
    a round needs BOTH R_A and R_B and increments a shared counter.
    """
    reset_clock()
    tracker = Tracker()
    A = TrackedLock("R_A", 1, tracker)
    B = TrackedLock("R_B", 2, tracker)
    plan = {"T1": (A, B), "T2": (B, A)}
    backoff = {"T1": 0.1, "T2": 0.8}  # asymmetric so the retries do not collide again
    shared = {"counter": 0, "aborted": 0, "timeouts": 0, "deadlocks_detected": 0}
    shared_lock = threading.Lock()
    barrier = threading.Barrier(2, timeout=60)  # start each round together (forces the overlap)

    def worker(name):
        first, second = plan[name]
        if mode == "ordered":
            first, second = sorted(plan[name], key=lambda l: l.rank)
        for rnd in range(1, rounds + 1):
            barrier.wait()
            attempt = 0
            while True:
                attempt += 1
                log(f"{name} round {rnd} attempt {attempt}: requesting {first.name}")
                if not first.acquire(name, timeout=10):
                    log(f"{name} could not get {first.name}; giving up round")
                    break
                log(f"{name} HOLDS {first.name}")
                if attempt == 1:
                    time.sleep(0.2)  # controlled interleaving: both threads now hold their first lock
                log(f"{name} requesting {second.name} while holding {first.name}")
                if second.acquire(name, timeout=timeout if mode != "ordered" else 10):
                    with shared_lock:
                        shared["counter"] += 1
                        c = shared["counter"]
                    log(f"{name} has BOTH locks -> critical section, shared counter = {c}")
                    time.sleep(0.05)
                    second.release()
                    first.release()
                    log(f"{name} released {second.name}, {first.name}")
                    break
                with shared_lock:
                    shared["timeouts"] += 1
                log(f"{name} TIMED OUT waiting for {second.name}; releasing {first.name} (rollback)")
                first.release()
                if mode == "recover":
                    log(f"{name} backs off {backoff[name]}s then retries")
                    time.sleep(backoff[name])
                    continue
                with shared_lock:
                    shared["aborted"] += 1
                log(f"{name} aborts round {rnd} (no recovery in this mode)")
                break

    threads = [threading.Thread(target=worker, args=(n,), name=n) for n in plan]
    for t in threads:
        t.start()
    flagged, explained = False, False
    while any(t.is_alive() for t in threads):  # watchdog = deadlock detector
        cyc = tracker.find_cycle()
        if cyc and not flagged:
            flagged = True
            shared["deadlocks_detected"] += 1
            log("*** DEADLOCK DETECTED by watchdog (cycle in wait-for graph) ***")
            if not explained:
                explain_coffman(cyc, tracker)
                explained = True
        elif not cyc:
            flagged = False
        time.sleep(0.05)
    for t in threads:
        t.join()
    expected = 2 * rounds
    shared["expected"] = expected
    return shared


def part_threads():
    banner("PART 1  Circular wait with threads: detection, recovery, prevention")
    results = {}
    for mode, title in [
        ("naive", "RUN A: naive lock order (T1: A->B, T2: B->A), timeout only, NO recovery"),
        ("recover", "RUN B: same order, deadlock detected, then RECOVERY (rollback + back-off + retry)"),
        ("ordered", "RUN C: PREVENTION by resource ordering (both threads: R_A then R_B)"),
    ]:
        print(f"\n--- {title} ---")
        results[mode] = run_scenario(mode)
    print("\n--- Summary (same workload in every run: 2 threads x 3 rounds) ---")
    print(f"{'run':<10}{'expected':>10}{'actual':>8}{'deadlocks':>11}{'timeouts':>10}{'aborted':>9}  result")
    for mode, r in results.items():
        ok = r["counter"] == r["expected"]
        print(f"{mode:<10}{r['expected']:>10}{r['counter']:>8}{r['deadlocks_detected']:>11}"
              f"{r['timeouts']:>10}{r['aborted']:>9}  "
              f"{'PASS (all work done)' if ok else 'FAIL (work lost to deadlock)'}")
    return results


# --------------------------------------------------------------------------
# PART 2: Banker's algorithm
# --------------------------------------------------------------------------
def need_matrix(alloc, mx):
    return [[mx[i][j] - alloc[i][j] for j in range(len(mx[i]))] for i in range(len(mx))]


def show_matrix(name, mat, res_names):
    print(f"  {name}:")
    print("        " + "  ".join(f"{r:>3}" for r in res_names))
    for i, row in enumerate(mat):
        print(f"    P{i}  " + "  ".join(f"{v:>3}" for v in row))


def validate(alloc, mx, avail):
    errors = []
    for i, (a, m) in enumerate(zip(alloc, mx)):
        for j, (x, y) in enumerate(zip(a, m)):
            if x < 0 or y < 0:
                errors.append(f"P{i} resource {j}: negative value")
            if x > y:
                errors.append(f"P{i} resource {j}: Allocation {x} > Maximum {y}")
    if any(v < 0 for v in avail):
        errors.append("Available contains a negative value")
    return errors


def safety(avail, alloc, need, verbose=True):
    """Banker's safety algorithm. Returns (is_safe, safe_sequence)."""
    n, m = len(alloc), len(avail)
    work, finish, seq = list(avail), [False] * n, []
    while len(seq) < n:
        progressed = False
        for i in range(n):
            if not finish[i] and all(need[i][j] <= work[j] for j in range(m)):
                if verbose:
                    print(f"    P{i}: Need {need[i]} <= Work {work}  -> can finish, "
                          f"releases {alloc[i]}", end="")
                work = [work[j] + alloc[i][j] for j in range(m)]
                if verbose:
                    print(f"  => Work = {work}")
                finish[i] = True
                seq.append(i)
                progressed = True
        if not progressed:
            break
    if verbose and len(seq) < n:
        stuck = [f"P{i}" for i in range(n) if not finish[i]]
        print(f"    No remaining process has Need <= Work {work}; stuck: {', '.join(stuck)}")
    return len(seq) == n, seq


def request_resources(pid, req, avail, alloc, need):
    """Resource-request algorithm: grant only if the resulting state is safe."""
    m = len(avail)
    if any(req[j] > need[pid][j] for j in range(m)):
        return False, "denied: request exceeds the process's declared maximum claim"
    if any(req[j] > avail[j] for j in range(m)):
        return False, "denied (P must wait): not enough Available resources"
    a2 = [avail[j] - req[j] for j in range(m)]
    al2 = [row[:] for row in alloc]
    nd2 = [row[:] for row in need]
    for j in range(m):
        al2[pid][j] += req[j]
        nd2[pid][j] -= req[j]
    print(f"    Pretend-allocate -> Available {a2}; running safety check:")
    ok, seq = safety(a2, al2, nd2)
    if ok:
        return True, "granted: state stays safe, sequence " + " -> ".join(f"P{i}" for i in seq)
    return False, "denied: granting would leave the system in an UNSAFE state"


def analyse_state(title, alloc, mx, avail, res_names):
    print(f"\n--- {title} ---")
    errs = validate(alloc, mx, avail)
    if errs:
        print("  Invalid input:", *errs, sep="\n    ")
        return None
    need = need_matrix(alloc, mx)
    show_matrix("Allocation", alloc, res_names)
    show_matrix("Maximum", mx, res_names)
    show_matrix("Need = Maximum - Allocation", need, res_names)
    print(f"  Available: {avail}")
    print("  Safety algorithm trace:")
    ok, seq = safety(avail, alloc, need)
    if ok:
        print("  RESULT: SAFE state. Safe sequence: <" + ", ".join(f"P{i}" for i in seq) + ">")
    else:
        print("  RESULT: UNSAFE state (no safe sequence exists; deadlock is possible)")
    return need, ok


def read_vec(prompt, m):
    while True:
        try:
            v = [int(x) for x in input(prompt).split()]
            if len(v) == m:
                return v
        except ValueError:
            pass
        print(f"  Please enter exactly {m} integers separated by spaces.")


def part_banker(interactive=False):
    banner("PART 2  Banker's algorithm (Need matrix, safety check, requests)")
    res = ["A", "B", "C"]
    alloc = [[0, 1, 0], [2, 0, 0], [3, 0, 2], [2, 1, 1], [0, 0, 2]]
    mx = [[7, 5, 3], [3, 2, 2], [9, 0, 2], [2, 2, 2], [4, 3, 3]]
    avail = [3, 3, 2]
    if interactive:
        n = int(input("Number of processes: "))
        m = int(input("Number of resource types: "))
        res = [chr(ord("A") + j) for j in range(m)]
        alloc = [read_vec(f"Allocation P{i} ({m} ints): ", m) for i in range(n)]
        mx = [read_vec(f"Maximum    P{i} ({m} ints): ", m) for i in range(n)]
        avail = read_vec(f"Available ({m} ints): ", m)
    out = analyse_state("Dataset 1" + (" (user input)" if interactive else " (textbook example)"),
                        alloc, mx, avail, res)
    if interactive or out is None:
        return
    need, _ = out
    analyse_state("Dataset 2: same Allocation/Maximum but Available = [0, 0, 0]  (expected UNSAFE)",
                  alloc, mx, [0, 0, 0], res)
    print("\n--- Resource-request algorithm on Dataset 1 ---")
    for pid, req in [(1, [1, 0, 2]), (4, [3, 3, 0]), (0, [0, 2, 0])]:
        print(f"  P{pid} requests {req}")
        ok, msg = request_resources(pid, req, avail, alloc, need)
        print(f"    -> {msg}")


# --------------------------------------------------------------------------
# PART 3: deadlock detection + recovery by termination
# --------------------------------------------------------------------------
def detect(avail, alloc, request, active):
    """Detection algorithm. Returns the list of deadlocked process ids."""
    n, m = len(alloc), len(avail)
    work = list(avail)
    finish = [(i not in active) or all(v == 0 for v in alloc[i]) for i in range(n)]
    changed = True
    while changed:
        changed = False
        for i in range(n):
            if not finish[i] and all(request[i][j] <= work[j] for j in range(m)):
                print(f"    P{i}: Request {request[i]} <= Work {work} -> can finish, "
                      f"reclaim {alloc[i]}", end="")
                work = [work[j] + alloc[i][j] for j in range(m)]
                print(f"  => Work = {work}")
                finish[i] = True
                changed = True
    return [i for i in range(n) if not finish[i]]


def part_detect():
    banner("PART 3  Deadlock detection and controlled recovery (process termination)")
    res = ["A", "B", "C"]
    alloc = [[0, 1, 0], [2, 0, 0], [3, 0, 3], [2, 1, 1], [0, 0, 2]]
    request = [[0, 0, 0], [2, 0, 2], [0, 0, 1], [1, 0, 0], [0, 0, 2]]  # P2 now also wants one C
    avail = [0, 0, 0]
    active = set(range(5))
    show_matrix("Allocation", alloc, res)
    show_matrix("Current Request", request, res)
    print(f"  Available: {avail}")
    step = 1
    while True:
        print(f"\n  Detection pass {step}:")
        dead = detect(avail, alloc, request, active)
        if not dead:
            print("  RESULT: no deadlock; all remaining processes can finish.")
            break
        print("  RESULT: DEADLOCK among " + ", ".join(f"P{i}" for i in dead))
        victim = max(dead, key=lambda i: sum(alloc[i]))  # policy: free the most resources
        print(f"  RECOVERY: terminate victim P{victim} "
              f"(policy: holds the most resources = {sum(alloc[victim])}); "
              f"releasing {alloc[victim]}")
        avail = [avail[j] + alloc[victim][j] for j in range(len(avail))]
        alloc[victim] = [0] * len(res)
        request[victim] = [0] * len(res)
        active.discard(victim)
        print(f"  Available after release: {avail}")
        step += 1
    print("  Terminated processes: " + (", ".join(f"P{i}" for i in range(5) if i not in active) or "none"))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--part", choices=["all", "threads", "banker", "detect"], default="all")
    ap.add_argument("--interactive", action="store_true", help="enter Banker's matrices by hand")
    args = ap.parse_args()
    if args.part in ("all", "threads"):
        part_threads()
    if args.part in ("all", "banker"):
        part_banker(args.interactive)
    if args.part in ("all", "detect"):
        part_detect()
    print("\nAll parts finished normally (no permanent deadlock).")


if __name__ == "__main__":
    main()
