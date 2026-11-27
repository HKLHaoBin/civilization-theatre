"""Fresh-process evidence runner; snapshots every parent input before execution."""
from pathlib import Path
import hashlib
import json
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT/'app/evidence/p023b4-B-parent-first-red-source'

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def snapshot(label):
    manifest = json.loads((BASE/'manifest.json').read_text(encoding='utf-8'))
    names = set(manifest['sha256'])
    names.update(str(p.relative_to(ROOT)).replace('\\','/') for p in (ROOT/'app/tests').glob('*dispatch*ack*.py'))
    names.add('app/tests/p023b4_b_cycle.py')
    destination = ROOT/'app/evidence'/('p023b4-backend-B-'+label+'-source')
    destination.mkdir(exist_ok=False)
    hashes = {}
    for name in sorted(names):
        original = ROOT/name
        target = destination/name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(original.read_bytes())
        hashes[name] = sha(original)
        assert sha(target) == hashes[name]
    assert set(manifest['sha256']) <= set(hashes)
    result = dict(source_files=len(hashes), parent_inputs=237, sha256=hashes,
                  all237_copied=True, stage_accepted=False, fullb4=False)
    (destination/'manifest.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    return destination

if __name__ == '__main__':
    label, *tests = sys.argv[1:]
    destination = snapshot(label)
    code = "import tempfile,unittest,sys,threading,json;tempfile.tempdir='C:/Users/We3q/AppData/Local/hermes/cache/scratch';s=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromName(n) for n in sys.argv[1:]);r=unittest.TextTestRunner(verbosity=2).run(s);print(json.dumps({'executed':r.testsRun,'failures':len(r.failures),'errors':len(r.errors),'alive_threads':[t.name for t in threading.enumerate() if t is not threading.main_thread()]}));sys.exit(not r.wasSuccessful())"
    command = [sys.executable,'-B','-c',code,*tests]
    (destination/'command.json').write_text(json.dumps(dict(command=command,cwd=str(ROOT)),indent=2),encoding='utf-8')
    prefix=ROOT/'app/evidence'/('p023b4-backend-B-'+label)
    with Path(str(prefix)+'.stdout.log').open('wb') as out, Path(str(prefix)+'.stderr.log').open('wb') as err:
        execution=subprocess.run(command,cwd=ROOT,stdout=out,stderr=err)
    report=dict(exit_code=execution.returncode,source_manifest=str(destination/'manifest.json'),tests=tests)
    (destination/'execution.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report))
    sys.exit(execution.returncode)
