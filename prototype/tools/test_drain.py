"""Drain lifecycle checks without models, microphone, or network access."""
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('drain', Path(__file__).parents[1] / 'app/drain.py')
drain = importlib.util.module_from_spec(spec)
spec.loader.exec_module(drain)


class DrainTest(unittest.TestCase):
    def run_wait(self, states, timeout=30, settle=2):
        clock = [0]
        reads = []

        def fetch(*args, **kwargs):
            reads.append(clock[0])
            item = states[min(len(reads) - 1, len(states) - 1)]
            if isinstance(item, Exception):
                raise item
            return io.BytesIO(json.dumps(item).encode())

        with tempfile.TemporaryDirectory() as directory:
            flag = Path(directory) / 'draining'
            with patch.object(drain, 'FLAG', flag), \
                 patch.object(drain.time, 'monotonic', side_effect=lambda: clock[0]), \
                 patch.object(drain.time, 'sleep', side_effect=lambda n: clock.__setitem__(0, clock[0] + n)), \
                 patch.object(drain.urllib.request, 'urlopen', side_effect=fetch):
                self.assertFalse(drain.draining())
                drain.wait_for_connection(timeout=timeout, settle=settle)
                self.assertTrue(drain.draining())
        return clock[0]

    def test_active_connection_is_kept_until_closed(self):
        self.assertEqual(self.run_wait([{'connected': True}] * 5 + [{'connected': False}]), 5)

    def test_idle_waits_for_route_propagation(self):
        self.assertEqual(self.run_wait([{'connected': False}]), 2)

    def test_missing_field_or_bad_health_cannot_end_connection_early(self):
        self.assertEqual(self.run_wait([{}, OSError('unavailable')], timeout=6), 6)

    def test_active_connection_has_finite_shutdown_bound(self):
        self.assertEqual(self.run_wait([{'connected': True}], timeout=6), 6)


if __name__ == '__main__':
    unittest.main()
