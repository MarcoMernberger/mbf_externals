#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Read more about conftest.py under:
    https://pytest.org/latest/plugins.html
"""

# import pytest
import sys
import os
from pathlib import Path
os.chdir(Path(__file__).parent.absolute())

from pypipegraph.testing.fixtures import (  # noqa:F401
    new_pipegraph,
    pytest_runtest_makereport,
    no_pipegraph,
)
from mbf_externals.testing.fixtures import (  # noqa:F401
    local_store,
    per_test_store,
    per_run_store,
)

root = Path(__file__).parent.parent
sys.path.append(str(root / "src"))
