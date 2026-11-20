"""Exercise publish success and failure paths without registry access."""
import json,os,subprocess,tempfile,unittest
from pathlib import Path
SCRIPT=Path(__file__).with_name('publish-dev-ecr.sh').resolve()
MOCK="#!/usr/bin/env python3\nimport json, os, sys\na=sys.argv[1:];case=os.environ['CASE'];digest=lambda c:'sha256:'+c*64\nwith open(os.environ['MOCK_LOG'],'a') as f:f.write(json.dumps(a)+'\\n')\nif a[:2]==['image','inspect']:\n remote='amazonaws.com/' in a[2]\n if a[-1]=='{{.Id}}':print(digest('f') if case=='wrong-pulled-id' and remote else digest('c'))\n else:print('linux/arm64' if case=='wrong-platform' else 'linux/amd64')\nelif a[0]=='tag':pass\nelif a[0]=='push':sys.exit(1 if case=='push-fails' else 0)\nelif a[0]=='pull':sys.exit(1 if case=='pull-fails' else 0)\nelif a[:3]==['buildx','imagetools','inspect']:\n if '--raw' in a:print(json.dumps({'config':{'digest':digest('f') if case=='wrong-config' else digest('c')},'layers':[]}))\n else:print('Digest: '+('invalid' if case=='bad-digest' else digest('e')))\nelse:sys.exit(99)\n"
class Publish(unittest.TestCase):
 def exercise(self,case='pass',overrides=None):
  with tempfile.TemporaryDirectory() as td:
   p=Path(td);fake=p/'docker';fake.write_text(MOCK);fake.chmod(0o755)
   env=dict(os.environ,PATH=str(p)+os.pathsep+os.environ['PATH'],CASE=case,MOCK_LOG=str(p/'calls'),GITHUB_REPOSITORY='nineOuting/nineouting-stt',GITHUB_REF='refs/heads/develop',GITHUB_EVENT_NAME='push',EXPECTED_ACCOUNT='443845329525',AWS_REGION='ap-northeast-2',LOCAL_IMAGE='nineouting-verified:local',TARGET_IMAGE='443845329525.dkr.ecr.ap-northeast-2.amazonaws.com/nineouting-dev-stt',GITHUB_SHA='1'*40,GITHUB_RUN_ID='42',GITHUB_RUN_ATTEMPT='1',GITHUB_OUTPUT=str(p/'output'),GITHUB_STEP_SUMMARY=str(p/'summary'))
   env.update(overrides or {})
   r=subprocess.run(['bash',str(SCRIPT)],cwd=p,env=env,capture_output=True,text=True)
   calls=[json.loads(l) for l in (p/'calls').read_text().splitlines()] if (p/'calls').exists() else []
   evidence=(p/'image-result.txt').read_text() if (p/'image-result.txt').exists() else ''
   output=(p/'output').read_text() if (p/'output').exists() else ''
   return r,calls,evidence,output
 def test_exact_local_image_is_published(self):
  r,calls,evidence,output=self.exercise();self.assertEqual(r.returncode,0,r.stderr)
  self.assertEqual(len(evidence.splitlines()),4)
  self.assertIn('image.digest=sha256:'+'e'*64,evidence)
  self.assertIn('image_digest=sha256:'+'e'*64,output)
  self.assertTrue(any(c[0]=='push' and 'amazonaws.com/' in c[1] for c in calls))
  self.assertFalse(any(c[:3]==['buildx','imagetools','create'] for c in calls))
 def test_failures_emit_no_verified_evidence(self):
  for case in ['wrong-config','wrong-pulled-id','wrong-platform','bad-digest','push-fails','pull-fails']:
   with self.subTest(case=case):
    r,_,evidence,output=self.exercise(case);self.assertNotEqual(r.returncode,0);self.assertEqual((evidence,output),('',''))
 def test_wrong_context_never_contacts_registry(self):
  for override in [{'GITHUB_REPOSITORY':'other/repo'},{'GITHUB_REF':'refs/heads/main'},{'GITHUB_EVENT_NAME':'pull_request'},{'EXPECTED_ACCOUNT':'000000000000'},{'TARGET_IMAGE':'other/image'},{'GITHUB_SHA':'latest'},{'LOCAL_IMAGE':'other:local'}]:
   with self.subTest(override=override):
    r,calls,_,_=self.exercise(overrides=override);self.assertNotEqual(r.returncode,0);self.assertEqual(calls,[])
if __name__=='__main__':unittest.main()
