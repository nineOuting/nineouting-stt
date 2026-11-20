"""새 회의를 받을지 정하는 판단 (app/ready.py, SCRUM-337). 모델 없이 돎."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('ready', Path(__file__).parents[1] / 'app/ready.py')
ready = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ready)
decide = ready.decide


def room(k, backlog, done=5.0, spent=5.0):
    return (k, backlog, done, spent)


class DecideTest(unittest.TestCase):
    def test_room_left_accepts(self):
        self.assertEqual(decide(False, 3, 4, 1, [room('1', 0.0)] * 3, 5), (False, ''))

    def test_full_rejects(self):
        busy, why = decide(False, 4, 4, 1, [room('1', 0.0)] * 4, 5)
        self.assertTrue(busy); self.assertIn('상한 4', why)

    def test_no_cap_never_full(self):
        self.assertFalse(decide(False, 50, 0, 1, [], 5)[0])

    def test_reopens_after_margin(self):
        self.assertFalse(decide(True, 3, 4, 1, [room('1', 0.2)], 5)[0])           # 1자리 비면 다시 받음
        self.assertTrue(decide(True, 3, 4, 2, [room('1', 0.2)], 5)[0])            # 여유 2면 아직
        self.assertFalse(decide(True, 2, 4, 2, [room('1', 0.2)], 5)[0])
        self.assertFalse(decide(True, 3, 4, 0, [room('1', 0.2)], 5)[0])           # 1 미만은 1로 봄

    def test_lag_when_slower_than_realtime(self):
        busy, why = decide(False, 2, 4, 1, [room('7', 6.2, done=3.0, spent=5.0), room('8', 0.0)], 5)
        self.assertTrue(busy); self.assertIn('회의 7', why)

    def test_catchup_is_not_lag(self):
        # 다시 붙은 회의가 모아 둔 소리를 몰아 보냄: 밀린 소리는 크지만 실시간보다 빨리 처리 중
        self.assertFalse(decide(False, 2, 4, 1, [room('7', 60.0, done=40.0, spent=5.0)], 5)[0])

    def test_unknown_progress_is_not_lag(self):
        # 첫 판단(지난 값 없음)이나 엔진이 아직 없을 때: server.py가 done=spent로 넣음
        self.assertFalse(decide(False, 1, 4, 1, [room('7', 30.0, done=0.0, spent=0.0)], 5)[0])

    def test_small_backlog_is_not_lag(self):
        self.assertFalse(decide(False, 1, 4, 1, [room('7', 4.0, done=1.0, spent=5.0)], 5)[0])

    def test_stays_busy_until_backlog_drains(self):
        busy, why = decide(True, 1, 4, 1, [room('7', 2.5, done=20.0, spent=5.0)], 5)
        self.assertTrue(busy); self.assertIn('1초 밑', why)
        self.assertFalse(decide(True, 1, 4, 1, [room('7', 0.4)], 5)[0])

    def test_full_and_lag_reports_full(self):
        busy, why = decide(False, 4, 4, 1, [room('7', 9.0, done=1.0)], 5)
        self.assertTrue(busy); self.assertIn('상한', why)


if __name__ == '__main__':
    unittest.main()
