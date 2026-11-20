"""Exercise real config-byte hashing without Docker, AWS, or a registry."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import io
import shutil
import tarfile
import uuid

SCRIPT = Path(__file__).with_name('publish-dev-ecr.sh').resolve()
CONFIG = b'{"architecture":"amd64","os":"linux","rootfs":{"type":"layers","diff_ids":[]}}'
CONFIG_DIGEST = 'sha256:' + hashlib.sha256(CONFIG).hexdigest()
MANIFEST_DIGEST = 'sha256:' + 'e' * 64
MOCK = r'''#!/usr/bin/env python3
import hashlib, io, json, os, sys, tarfile
a = sys.argv[1:]
case = os.environ['CASE']
config = b'{"architecture":"amd64","os":"linux","rootfs":{"type":"layers","diff_ids":[]}}'
config_digest = 'sha256:' + hashlib.sha256(config).hexdigest()
config_digest = os.environ.get('REAL_CONFIG_DIGEST', config_digest)
manifest_digest = 'sha256:' + 'e' * 64
with open(os.environ['MOCK_LOG'], 'a') as log:
    log.write(json.dumps(a) + '\n')
if a[:2] == ['image', 'inspect']:
    remote = 'amazonaws.com/' in a[2]
    if os.environ.get('REAL_DOCKER_BIN'):
        import subprocess
        sys.exit(subprocess.call([os.environ['REAL_DOCKER_BIN'], 'image', 'inspect', os.environ['REAL_DOCKER_IMAGE']] + a[3:]))
    if a[-1] == '{{.Id}}':
        print(config_digest if case == 'classic' else manifest_digest)
    else:
        print('linux/arm64' if case == 'wrong-platform' or (case == 'wrong-pulled-platform' and remote) else 'linux/amd64')
elif a[:2] == ['image', 'save']:
    remote = 'amazonaws.com/' in a[-1]
    if os.environ.get('REAL_DOCKER_BIN'):
        import subprocess
        sys.exit(subprocess.call([os.environ['REAL_DOCKER_BIN'], 'image', 'save', '--output', a[a.index('--output') + 1], os.environ['REAL_DOCKER_IMAGE']]))
    if case == 'save-fails' or (case == 'pulled-save-fails' and remote):
        sys.exit(1)
    if case == 'wrong-pulled-config' and remote:
        config = b'{"architecture":"amd64","os":"linux","changed":true}'
    config_hash = hashlib.sha256(config).hexdigest()
    path = config_hash + '.json' if case == 'classic' else 'blobs/sha256/' + config_hash
    manifest = [{'Config': path, 'RepoTags': [a[-1]], 'Layers': []}]
    if case == 'multiple-manifests':
        manifest *= 2
    with tarfile.open(a[a.index('--output') + 1], 'w') as archive:
        files = {'manifest.json': json.dumps(manifest).encode(), path: config}
        if case == 'missing-config':
            del files[path]
        for name, content in files.items():
            entry = tarfile.TarInfo(name)
            entry.size = len(content)
            archive.addfile(entry, io.BytesIO(content))
elif a[0] == 'tag':
    pass
elif a[0] == 'push':
    sys.exit(1 if case == 'push-fails' else 0)
elif a[0] == 'pull':
    sys.exit(1 if case == 'pull-fails' else 0)
elif a[:3] == ['buildx', 'imagetools', 'inspect']:
    if '--raw' in a:
        value = 'sha256:' + 'f' * 64 if case == 'wrong-config' else config_digest
        if case == 'bad-config-digest':
            value = 'invalid'
        print(json.dumps({} if case == 'missing-remote-config' else {'config': {'digest': value}, 'layers': []}))
    else:
        print('Digest: ' + ('invalid' if case == 'bad-digest' else manifest_digest))
else:
    sys.exit(99)
'''


class Publish(unittest.TestCase):
    def exercise(self, case='containerd', overrides=None):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            docker = root / 'docker'
            docker.write_text(MOCK)
            docker.chmod(0o755)
            env = dict(
                os.environ, PATH=str(root) + os.pathsep + os.environ['PATH'],
                CASE=case, MOCK_LOG=str(root / 'calls'),
                GITHUB_REPOSITORY='nineOuting/nineouting-stt',
                GITHUB_REF='refs/heads/develop', GITHUB_EVENT_NAME='push',
                EXPECTED_ACCOUNT='443845329525', AWS_REGION='ap-northeast-2',
                LOCAL_IMAGE='nineouting-verified:local',
                TARGET_IMAGE='443845329525.dkr.ecr.ap-northeast-2.amazonaws.com/nineouting-dev-stt',
                GITHUB_SHA='1' * 40, GITHUB_RUN_ID='42', GITHUB_RUN_ATTEMPT='1',
                GITHUB_OUTPUT=str(root / 'output'), GITHUB_STEP_SUMMARY=str(root / 'summary'),
            )
            env.update(overrides or {})
            result = subprocess.run(['bash', str(SCRIPT)], cwd=root, env=env,
                                    capture_output=True, text=True, timeout=30)
            calls = [json.loads(line) for line in (root / 'calls').read_text().splitlines()] if (root / 'calls').exists() else []
            read = lambda name: (root / name).read_text() if (root / name).exists() else ''
            return result, calls, read('image-result.txt'), read('output'), read('summary')

    def test_containerd_manifest_id_differs_from_config(self):
        self.assertNotEqual(MANIFEST_DIGEST, CONFIG_DIGEST)
        self.assert_success('containerd')

    def test_classic_config_archive(self):
        self.assert_success('classic')

    @unittest.skipUnless(os.environ.get('PUBLISH_REAL_DOCKER') == '1', '실제 Docker 시험은 CI에서 실행')
    def test_real_runner_docker_config_archive(self):
        docker = shutil.which('docker')
        self.assertIsNotNone(docker)
        image = 'ecr-config-regression:' + uuid.uuid4().hex
        try:
            with tempfile.TemporaryDirectory() as td:
                Path(td, 'marker').write_text('config-digest-regression\n')
                Path(td, 'Dockerfile').write_text('FROM scratch\nCOPY marker /marker\nLABEL regression=config-digest\n')
                subprocess.run([docker, 'build', '--network=none', '--platform=linux/amd64', '-t', image, td], check=True, capture_output=True)
            saved = subprocess.run([docker, 'image', 'save', image], check=True, capture_output=True).stdout
            with tarfile.open(fileobj=io.BytesIO(saved)) as archive:
                manifest = json.load(archive.extractfile('manifest.json'))
                config_bytes = archive.extractfile(manifest[0]['Config']).read()
            config_digest = 'sha256:' + hashlib.sha256(config_bytes).hexdigest()
            result, _, evidence, output, _ = self.exercise(overrides={
                'REAL_DOCKER_BIN': docker, 'REAL_DOCKER_IMAGE': image,
                'REAL_CONFIG_DIGEST': config_digest,
            })
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn('image.digest=' + MANIFEST_DIGEST, evidence)
            self.assertIn('image_digest=' + MANIFEST_DIGEST, output)
        finally:
            subprocess.run([docker, 'image', 'rm', image], capture_output=True)

    def assert_success(self, case):
        result, calls, evidence, output, summary = self.exercise(case)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(evidence.splitlines()), 4)
        self.assertIn('image.digest=' + MANIFEST_DIGEST, evidence)
        self.assertIn('image_digest=' + MANIFEST_DIGEST, output)
        self.assertIn('config digests: MATCH', summary)
        self.assertEqual(sum(call[:2] == ['image', 'save'] for call in calls), 2)
        self.assertFalse(any('{{.Id}}' in call for call in calls))
        self.assertTrue(any(call[0] == 'push' and 'amazonaws.com/' in call[1] for call in calls))
        self.assertIn(['pull', '--platform', 'linux/amd64',
                       '443845329525.dkr.ecr.ap-northeast-2.amazonaws.com/nineouting-dev-stt@' + MANIFEST_DIGEST], calls)
        self.assertFalse(any(call[:3] == ['buildx', 'imagetools', 'create'] for call in calls))

    def test_failures_emit_no_verified_evidence(self):
        cases = {
            'wrong-config': 'Local/ECR config digest mismatch',
            'wrong-pulled-config': 'Local/pulled config digest mismatch',
            'wrong-platform': 'Local platform mismatch',
            'wrong-pulled-platform': 'Pulled platform mismatch',
            'bad-digest': 'Invalid publish context or digest',
            'bad-config-digest': 'Invalid ECR config digest',
            'missing-remote-config': 'ECR manifest has no config digest',
            'push-fails': 'ECR push failed',
            'pull-fails': 'ECR pull failed',
            'save-fails': 'Local image: could not save image',
            'pulled-save-fails': 'Pulled image: could not save image',
            'multiple-manifests': 'Local image: could not read saved image config',
            'missing-config': 'Local image: could not read saved image config',
        }
        for case, message in cases.items():
            with self.subTest(case=case):
                result, calls, evidence, output, summary = self.exercise(case)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('::error::' + message, result.stderr + result.stdout)
                self.assertEqual((evidence, output, summary), ('', '', ''))
                if case in ['wrong-platform', 'save-fails', 'multiple-manifests', 'missing-config']:
                    self.assertFalse(any(call[0] in ['push', 'pull'] for call in calls))

    def test_mismatch_names_expected_and_actual_config(self):
        result, *_ = self.exercise('wrong-config')
        self.assertIn('expected=' + CONFIG_DIGEST, result.stderr)
        self.assertIn('actual=sha256:' + 'f' * 64, result.stderr)

    def test_wrong_context_never_contacts_registry(self):
        for override in [
            {'GITHUB_REPOSITORY': 'other/repo'}, {'GITHUB_REF': 'refs/heads/main'},
            {'GITHUB_EVENT_NAME': 'pull_request'}, {'EXPECTED_ACCOUNT': '000000000000'},
            {'AWS_REGION': 'us-east-1'}, {'TARGET_IMAGE': 'other/image'},
            {'GITHUB_SHA': 'latest'}, {'LOCAL_IMAGE': 'other:local'},
            {'GITHUB_RUN_ID': 'bad'}, {'GITHUB_RUN_ATTEMPT': 'bad'},
        ]:
            with self.subTest(override=override):
                result, calls, evidence, output, summary = self.exercise(overrides=override)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(calls, [])
                self.assertEqual((evidence, output, summary), ('', '', ''))


if __name__ == '__main__':
    unittest.main()
