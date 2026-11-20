import unittest
import deadlock_bankers as d

ALLOC = [[0, 1, 0], [2, 0, 0], [3, 0, 2], [2, 1, 1], [0, 0, 2]]
MAX = [[7, 5, 3], [3, 2, 2], [9, 0, 2], [2, 2, 2], [4, 3, 3]]


class BankerTests(unittest.TestCase):
    def test_need(self):
        self.assertEqual(d.need_matrix(ALLOC, MAX)[0], [7, 4, 3])

    def test_safe(self):
        ok, seq = d.safety([3, 3, 2], ALLOC, d.need_matrix(ALLOC, MAX), verbose=False)
        self.assertTrue(ok)
        self.assertEqual(seq, [1, 3, 4, 0, 2])

    def test_unsafe(self):
        ok, _ = d.safety([0, 0, 0], ALLOC, d.need_matrix(ALLOC, MAX), verbose=False)
        self.assertFalse(ok)

    def test_request(self):
        need = d.need_matrix(ALLOC, MAX)
        self.assertTrue(d.request_resources(1, [1, 0, 2], [3, 3, 2], ALLOC, need)[0])
        self.assertFalse(d.request_resources(4, [3, 3, 0], [3, 3, 2], ALLOC, need)[0])


class ThreadTests(unittest.TestCase):
    def test_modes(self):
        n = d.run_scenario("naive", rounds=1, timeout=0.5)
        self.assertEqual(n["counter"], 0)
        self.assertGreaterEqual(n["deadlocks_detected"], 1)
        r = d.run_scenario("recover", rounds=1, timeout=0.5)
        self.assertEqual(r["counter"], r["expected"])
        o = d.run_scenario("ordered", rounds=2)
        self.assertEqual(o["counter"], o["expected"])
        self.assertEqual(o["deadlocks_detected"], 0)


if __name__ == "__main__":
    unittest.main()
