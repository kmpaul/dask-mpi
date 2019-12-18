from __future__ import print_function, division, absolute_import

import os
import sys
import subprocess
import requests

import pytest

pytest.importorskip("mpi4py")


def subprocess_returncode(mpirun, name):
    root = os.path.dirname(os.path.realpath(__file__))
    script = os.path.join(root, "core_scripts", name)
    p = subprocess.Popen(mpirun + ["-np", "4", sys.executable, script])
    p.wait()
    return p.returncode


def test_basic(mpirun):
    assert subprocess_returncode(mpirun, "basic.py") == 0


def test_no_nanny(mpirun):
    assert subprocess_returncode(mpirun, "no_nanny.py") == 0


def test_dashboard(mpirun):
    assert subprocess_returncode(mpirun, "dashboard.py") == 0
    with pytest.raises(Exception):
        requests.get("http://localhost:59583/status/")
