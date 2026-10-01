"""Run the project's pytest suite with one injected mutant; print failing tests."""
import sys, io, contextlib
from pathlib import Path
root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root / "src")); sys.path.insert(0, str(root / "experiments"))
import pytest, mutants
name = sys.argv[1]
undo = mutants.apply(name)
class P:
    def __init__(self): self.failed=[]; self.passed=0
    def pytest_runtest_logreport(self, report):
        r = report
        if r.when=="call":
            if r.failed: self.failed.append(r.nodeid.split("::")[-1])
            elif r.passed: self.passed+=1
p=P()
with contextlib.redirect_stdout(io.StringIO()):
    pytest.main([str(root/"tests"),"-q","-p","no:cacheprovider"], plugins=[p])
undo()
print(f"mutant={name} passed={p.passed} failed={len(p.failed)} {p.failed}")
