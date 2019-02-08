import pytest
from pathlib import Path
import shutil


@pytest.fixture(scope="class")
def local_store():
    from mbf_externals import ExternalAlgorithmStore, change_global_store

    base = Path(__file__).parent.parent.parent.parent / "tests"
    unpacked = base / "unpacked"
    if unpacked.exists():
        shutil.rmtree(unpacked)
    unpacked.mkdir()
    store = ExternalAlgorithmStore(base / "zipped", unpacked)
    change_global_store(store)
    yield store
    if unpacked.exists():
        shutil.rmtree(unpacked)


@pytest.fixture(scope="class")
def global_store():
    from mbf_externals import create_defaults

    store = create_defaults()
    yield store
