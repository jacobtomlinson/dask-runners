import os
import subprocess
import sys

import pytest

pytest.skip()  # Skip SLURM tests which are currently hanging


def test_context(srun):
    script_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "slurm_core_context.py")

    p = subprocess.Popen(srun + ["-n", "4", sys.executable, script_file])

    p.communicate()
    assert p.returncode == 0


def test_small_world(srun):
    script_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "slurm_core_context.py")

    p = subprocess.Popen(srun + ["-n", "1", sys.executable, script_file], stderr=subprocess.PIPE)

    _, std_err = p.communicate()
    assert p.returncode != 0
    assert "Not enough Slurm tasks" in std_err.decode(sys.getfilesystemencoding())
