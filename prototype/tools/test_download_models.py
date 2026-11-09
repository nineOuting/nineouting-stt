"""Offline tests: transient retries must never bypass integrity checks."""
import contextlib
import hashlib
import io
import os
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
import download_models as m

class DownloadTests(unittest.TestCase):
    def test_504_then_success(self):
        with patch.object(m, 'download_once', side_effect=[HTTPError('u',504,'timeout',{},None),'file.part']) as call, patch.object(m.time,'sleep'):
            self.assertEqual(m.download('u','file',1),'file.part')
            self.assertEqual(call.call_count,2)

    def test_exhaustion_cleans_partial(self):
        with tempfile.TemporaryDirectory() as d:
            dest=os.path.join(d,'model')
            def fail(*args):
                with open(dest+'.part','w') as f: f.write('partial')
                raise TimeoutError('timeout')
            with patch.object(m,'download_once',side_effect=fail) as call, patch.object(m.time,'sleep'):
                with self.assertRaises(TimeoutError):m.download('u',dest,1)
                self.assertEqual(call.call_count,3)
            self.assertFalse(os.path.exists(dest+'.part'))

    def test_404_not_retried(self):
        with patch.object(m,'download_once',side_effect=HTTPError('u',404,'missing',{},None)) as call:
            with self.assertRaises(HTTPError):m.download('u','file',1)
            self.assertEqual(call.call_count,1)

    def test_checksum_failure_still_fails(self):
        with tempfile.TemporaryDirectory() as d:
            def corrupt(url,dest,size):
                with open(dest+'.part','wb') as f:f.write(b'bad')
                return dest+'.part'
            model=[('필수','model','u',hashlib.sha256(b'good').hexdigest(),4)]
            with patch.object(m,'MODELS',model), patch.object(m,'download',side_effect=corrupt), patch('sys.argv',['download_models','--dir',d]), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(SystemExit) as e:m.main()
                self.assertEqual(e.exception.code,1)
            self.assertFalse(os.path.exists(os.path.join(d,'model')))
            self.assertFalse(os.path.exists(os.path.join(d,'model.part')))

if __name__ == '__main__':unittest.main()
