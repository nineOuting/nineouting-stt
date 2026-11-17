"""Load only HTTP hooks; never start a model, server, microphone, or backend."""
import ast
import json
from pathlib import Path
import threading
from types import SimpleNamespace
import unittest

source = Path(__file__).parents[1] / 'app/server.py'
tree = ast.parse(source.read_text())
functions = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in ('health', 'ws_process_request', 'full')]


class RouteTest(unittest.TestCase):
    def setUp(self):
        self.is_draining = False
        self.conn = object()
        self.ns = {'json': json, 'PROC': {'running': True, 'error': None}, 'A': SimpleNamespace(input='ws', max_meetings=2),
                   'WS_MODE': True, 'LOCAL': None, 'MLOCK': threading.Lock(), 'MEETINGS': {'2': object()},
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

    def test_full_meetings_make_not_ready_but_healthy(self):
        self.ns['MEETINGS']['3'] = object()   # 상한 2에 닿음
        self.assertEqual(self.request('/ready').status_code, 503)
        self.assertEqual(json.loads(self.request('/ready').body)['full'], True)
        self.assertEqual(self.request('/health').status_code, 200)
        self.assertEqual(json.loads(self.request('/health').body)['meetings'], ['2', '3'])
        self.assertIsNone(self.request('/ws/stt/meetings/4?ticket=test-only'))   # 거절(1013)은 손잡기 뒤 ws_handler에서
        del self.ns['MEETINGS']['3']
        self.assertEqual(self.request('/ready').status_code, 200)


if __name__ == '__main__':
    unittest.main()
