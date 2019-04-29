#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Read more about conftest.py under:
    https://pytest.org/latest/plugins.html
"""

# import pytest
import sys
from pathlib import Path

from pypipegraph.testing.fixtures import (  # noqa:F401
    new_pipegraph,
    pytest_runtest_makereport,
    no_pipegraph,
)
from mbf_externals.testing.fixtures import local_store, global_store  # noqa:F401

root = Path(__file__).parent.parent
sys.path.append(str(root / "src"))
