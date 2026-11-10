"""Load only HTTP hooks; never start a model, server, microphone, or backend."""
import ast
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

source = Path(__file__).parents[1] / 'app/server.py'
tree = ast.parse(source.read_text())
functions = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in ('health', 'ws_process_request')]


class RouteTest(unittest.TestCase):
    def setUp(self):
        self.is_draining = False
        self.ns = {'json': json, 'status': {'running': True, 'state': 'run', 'audio_sec': 20},
                   'A': SimpleNamespace(input='ws'), 'SESS': {'meeting_id': '2'},
                   'WS': {'conn': object()}, 'draining': lambda: self.is_draining}
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
        connection = self.ns['WS']['conn']
        self.is_draining = True
        self.request('/ready')
        self.assertIs(self.ns['WS']['conn'], connection)
        self.assertTrue(json.loads(self.request('/health').body)['connected'])

    def test_dead_engine_is_unhealthy_even_without_drain(self):
        self.ns['status']['running'] = False
        self.assertEqual(self.request('/health').status_code, 503)


if __name__ == '__main__':
    unittest.main()
