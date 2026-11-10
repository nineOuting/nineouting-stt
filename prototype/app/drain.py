"""DEV rolling replacement: reject new connections, keep an existing meeting alive."""
import json
import os
from pathlib import Path
import time
import urllib.request

FLAG = Path('/tmp/stt-draining')


def draining():
    return FLAG.exists()


def wait_for_connection(timeout=3500, poll=1, settle=20):
    """Called by preStop. Bounded by Kubernetes grace and ALB deregistration delay.

    The bound is deliberate: this does not promise infinite meeting preservation.
    Existing connections stay on this process; new ones must use the new Ready pod.
    """
    FLAG.touch()
    started = time.monotonic()
    url = 'http://127.0.0.1:' + os.environ.get('WS_PORT', '8770') + '/health'
    while time.monotonic() - started < timeout:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                connected = json.load(response).get('connected', True)
            if not connected and time.monotonic() - started >= settle:
                return
        except (OSError, ValueError):
            # Failed health must not tear down an otherwise usable stream early.
            pass
        time.sleep(poll)


if __name__ == '__main__':
    wait_for_connection()
