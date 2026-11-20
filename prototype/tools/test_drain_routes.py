"""Load only HTTP hooks; never start a model, server, microphone, or backend."""
import ast
import importlib.util
import json
from pathlib import Path
import threading
from types import SimpleNamespace
import unittest

source = Path(__file__).parents[1] / 'app/server.py'
tree = ast.parse(source.read_text())
functions = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in ('health', 'ws_process_request', 'full', 'at_cap', 'update_ready')]
spec = importlib.util.spec_from_file_location('ready', Path(__file__).parents[1] / 'app/ready.py')
readiness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(readiness)


class RouteTest(unittest.TestCase):
    def setUp(self):
        self.is_draining = False
        self.conn = object()
        self.logs = []
        self.ns = {'json': json, 'PROC': {'running': True, 'error': None},
                   'A': SimpleNamespace(input='ws', max_meetings=2, ready_when_full=False, ready_margin=1, ready_backlog=5.0),
                   'READY': {'busy': False, 'reason': ''}, 'RESUMED': [0], 'readiness': readiness, 'log': self.logs.append,
                   'WS_MODE': True, 'LOCAL': None, 'MLOCK': threading.Lock(), 'MEETINGS': {'2': SimpleNamespace(q=SimpleNamespace(qsize=lambda: 0))},
                   'CONNS': {self.conn}, 'ROOM_ERR': {'count': 0, 'last': None}, 'draining': lambda: self.is_draining}
        exec(compile(ast.Module(body=functions, type_ignores=[]), str(source), 'exec'), self.ns)

    def request(self, path):
        return self.ns['ws_process_request'](None, SimpleNamespace(path=path))

    def test_ready_and_liveness_are_distinct_while_draining(self):
        self.assertEqual(self.request('/ready').status_code, 200)
        self.is_draining = True
        self.assertEqual(self.request('/ready').status_code, 503)
        self.assertEqual(self.request('/health').status_code, 200)

    def test_new_websocket_is_rejected_before_authentication(self):
        self.assertIsNone(self.request('/ws/stt/meetings/2?ticket=test-only'))
        self.is_draining = True
        self.assertEqual(self.request('/ws/stt/meetings/2?ticket=test-only').status_code, 503)

    def test_existing_connection_is_not_closed_by_health_checks(self):
        self.is_draining = True
        self.request('/ready')
        self.assertIn(self.conn, self.ns['CONNS'])
        self.assertTrue(json.loads(self.request('/health').body)['connected'])

    def test_dead_engine_is_unhealthy_even_without_drain(self):
        self.ns['PROC']['running'] = False
        self.assertEqual(self.request('/health').status_code, 503)

    def test_full_meetings_stay_ready(self):
        # 파드가 하나일 때 /ready가 503이면 ALB 대상에서 빠져 진행 중인 회의도 다시 붙지 못함 → 다 차도 200
        self.ns['MEETINGS']['3'] = SimpleNamespace(q=SimpleNamespace(qsize=lambda: 12))   # 상한 2에 닿음
        self.assertEqual(self.request('/ready').status_code, 200)
        h = json.loads(self.request('/health').body)
        self.assertEqual((h['meetings'], h['full'], h['backlog']['3']), (['2', '3'], True, 1.2))
        self.assertIsNone(self.request('/ws/stt/meetings/4?ticket=test-only'))   # 거절(1013)은 손잡기 뒤 ws_handler에서

    def test_zero_cap_means_no_limit(self):
        # MAX_MEETINGS=0: 상한 없음 (0을 넣었다고 모든 회의가 거절되면 안 됨)
        self.ns['A'].max_meetings = 0
        self.ns['MEETINGS']['3'] = SimpleNamespace(q=SimpleNamespace(qsize=lambda: 0))
        self.assertFalse(self.ns['full']())
        h = json.loads(self.request('/health').body)
        self.assertEqual((h['maxMeetings'], h['full']), (0, False))


    # ---- SCRUM-337: --ready-when-full ----
    def test_ready_when_full_off_ignores_busy(self):
        # 꺼져 있으면(기본) 지금과 같음: 판단 값이 어떻든 /ready는 drain만 봄, /health에 ready 없음
        self.ns['READY'].update(busy=True, reason='회의 2개 (상한 2)')
        self.assertEqual(self.request('/ready').status_code, 200)
        h = json.loads(self.request('/health').body)
        self.assertNotIn('ready', h); self.assertEqual(h['resumed'], 0)

    def test_ready_when_full_on(self):
        self.ns['A'].ready_when_full = True
        self.assertEqual(self.request('/ready').status_code, 200)
        self.assertTrue(json.loads(self.request('/health').body)['ready'])
        self.ns['READY'].update(busy=True, reason='회의 2개 (상한 2)')
        r = self.request('/ready')
        self.assertEqual(r.status_code, 503); self.assertIn('상한 2', json.loads(r.body)['reason'])
        h = json.loads(self.request('/health').body)
        self.assertEqual((h['ready'], h['busyReason']), (False, '회의 2개 (상한 2)'))
        self.assertEqual(self.request('/health').status_code, 200)   # 받지 않는 중이어도 살아 있음
        self.ns['READY'].update(busy=False, reason=''); self.is_draining = True
        self.assertEqual(self.request('/ready').status_code, 503)   # drain은 늘 먼저

    def test_update_ready_uses_processing_speed(self):
        self.ns['A'].ready_when_full = True; self.ns['A'].max_meetings = 4
        eng = SimpleNamespace(fed=0.0); q = SimpleNamespace(n=0); q.qsize = lambda: q.n
        r = SimpleNamespace(key='7', q=q, engine=eng, ready_prev=None)
        up = self.ns['update_ready']
        q.n = 300; up([r], 1, 100.0)                        # 첫 판단: 지난 값이 없어 밀림으로 보지 않음
        self.assertFalse(self.ns['READY']['busy'])
        eng.fed = 40.0; up([r], 1, 105.0)                   # 밀린 30초지만 5초에 40초를 처리 (몰아 보내기)
        self.assertFalse(self.ns['READY']['busy'])
        eng.fed = 43.0; up([r], 1, 110.0)                   # 5초에 3초만 처리: 실시간보다 느림
        self.assertTrue(self.ns['READY']['busy']); self.assertIn('회의 7', self.ns['READY']['reason'])
        eng.fed = 60.0; q.n = 5; up([r], 1, 115.0)          # 밀린 소리 0.5초로 내려옴
        self.assertFalse(self.ns['READY']['busy'])
        self.assertEqual(len(self.logs), 2)                 # 바뀔 때만 로그
        r.engine = None; q.n = 300; up([r], 1, 120.0)       # 엔진이 없으면 밀림으로 보지 않음
        self.assertFalse(self.ns['READY']['busy'])

if __name__ == '__main__':
    unittest.main()
