import os
import subprocess
import sys

import pytest

pytest.importorskip("mpi4py")


def test_context(mpirun):
    script_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "mpi_core_context.py")

    p = subprocess.Popen(mpirun + ["-np", "4", sys.executable, script_file])

    p.communicate()
    assert p.returncode == 0


def test_small_world(mpirun):
    script_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "mpi_core_context.py")

    p = subprocess.Popen(mpirun + ["-np", "1", sys.executable, script_file], stderr=subprocess.PIPE)

    _, std_err = p.communicate()
    assert p.returncode != 0
    assert "Not enough MPI ranks" in std_err.decode(sys.getfilesystemencoding())
