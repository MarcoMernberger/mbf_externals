import pytest
from pathlib import Path
import shutil


@pytest.fixture(scope="class")
def local_store():
    from mbf_externals import ExternalAlgorithmStore, change_global_store

    unpacked = Path(__file__).parent / "unpacked"
    if unpacked.exists():
        shutil.rmtree(unpacked)
    unpacked.mkdir()
    store = ExternalAlgorithmStore(Path(__file__).parent / "zipped", unpacked)
    change_global_store(store)
    yield store
    if unpacked.exists():
        shutil.rmtree(unpacked)


@pytest.fixture(scope="class")
def global_store():
    from mbf_externals import virtual_env_store

    store = virtual_env_store()
    yield store
