# Deadlock Handling and Banker's Algorithm (Python threads)

Run: `python deadlock_bankers.py` (all parts) | `--part threads|banker|detect` | `--interactive` (type your own Banker's matrices)
Tests: `python -m unittest -v test_deadlock`
Evidence: `test_output.txt` (full run log), `test_output_unittest.txt` (unit tests)

## What each part shows
| Requirement | Where |
|---|---|
| Controlled circular wait, no permanent block | Run A: T1 takes R_A→R_B, T2 takes R_B→R_A; a Barrier + 0.2 s sleep force the overlap; every second acquire has a timeout |
| Four Coffman conditions | printed by the watchdog at first detection (Runs A, B) |
| Detection (threads) | watchdog builds a wait-for graph from lock-holder / waiter records and reports the cycle |
| Recovery (threads) | Run B: timed-out thread rolls back (releases its lock), backs off (T1 0.1 s, T2 0.8 s) and retries |
| Prevention | Run C: both threads acquire in global order R_A < R_B, which breaks circular wait |
| Allocation / Maximum / Available input, Need matrix | Part 2 (built-in data or `--interactive`, with input validation) |
| Safety algorithm, safe sequence / unsafe report | Part 2, Dataset 1 safe `<P1,P3,P4,P0,P2>`, Dataset 2 unsafe |
| Resource-request algorithm | Part 2: P1 [1,0,2] granted, P4 [3,3,0] denied (unsafe), P0 [0,2,0] granted |
| Detection + recovery by termination | Part 3: P1–P4 deadlocked, P2 terminated (holds most resources), re-check shows no deadlock |

## Observed results (same workload: 2 threads x 3 rounds, expected counter = 6)
| run | expected | actual | deadlocks detected | timeouts | aborted rounds |
|---|---|---|---|---|---|
| naive | 6 | 3 | 3 | 3 | 3 |
| recover | 6 | 6 | 3 | 3 | 0 |
| ordered | 6 | 6 | 0 | 0 | 0 |

## Observations
- **Deadlock:** in Run A both threads hold one lock and wait for the other's; with plain `acquire()` this would hang forever. The timeout turns a permanent block into a detectable, bounded stall (about 1 s per round).
- **Naive still makes partial progress:** when one thread times out and releases its lock, the other finishes its round. The timed-out thread's work is lost (3 of 6), which is why a timeout alone is not a real solution.
- **Recovery** keeps all work (6/6) but still pays the deadlock cost on every round (3 detections, about 1 s each), so total time is about 4.75 s vs 1.5 s for prevention.
- **Prevention** never deadlocks: the loser of the race for R_A simply waits, so there is no hold-and-wait cycle. Cost: threads are serialised and a thread must know all resources' order in advance.
- **Fairness / starvation:** in Run B, T2's longer back-off means T1 wins retries. With a fixed asymmetry T2 could in principle be delayed repeatedly; randomised back-off would be fairer. In Run C the order of service is decided by who wins R_A first (alternates here).
- **Banker's:** safe means a sequence exists where every process can finish; unsafe does not mean deadlock has happened, only that it is possible (Dataset 2 with nothing available).
- **Detection vs avoidance:** Banker's needs maximum claims in advance; the detection algorithm uses only current requests, so it allows deadlock, then recovers (victim policy: most resources held, to free the most with one termination).
- **Race conditions:** the shared counter is only touched inside `shared_lock`, so the actual count equals the expected count whenever all rounds complete.
