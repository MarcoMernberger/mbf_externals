#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Read more about conftest.py under:
    https://pytest.org/latest/plugins.html
"""

# import pytest
import sys
from pathlib import Path
import shutil
import os
import pytest
from pypipegraph.tests.fixtures import new_pipegraph  # noqa:F401
from mbf_externals.tests.fixtures import local_store, global_store

root = Path(__file__).parent.parent
sys.path.append(str(root / "src"))
