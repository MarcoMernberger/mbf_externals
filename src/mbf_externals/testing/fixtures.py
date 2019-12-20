import pytest
from pathlib import Path
import shutil


@pytest.fixture(scope="class")
def local_store():
    from mbf_externals import ExternalAlgorithmStore, change_global_store

    base = Path(__file__).parent.parent.parent.parent / "tests"
    unpacked = base / "unpacked"
    if unpacked.exists():  # pragma: no cover
        shutil.rmtree(unpacked)
    unpacked.mkdir()
    store = ExternalAlgorithmStore(base / "zipped", unpacked, no_downloads=True)
    change_global_store(store)
    yield store
    if unpacked.exists():
        shutil.rmtree(unpacked)


@pytest.fixture
def per_test_store():
    from mbf_externals import ExternalAlgorithmStore, change_global_store

    base = Path("store").absolute()
    unpacked = base / "unpacked"
    if unpacked.exists():  # pragma: no cover
        shutil.rmtree(unpacked)
    unpacked.mkdir(exist_ok=False, parents=True)
    (base / "zipped").mkdir(exist_ok=True)
    store = ExternalAlgorithmStore(base / "zipped", unpacked)
    change_global_store(store)
    yield store
    if unpacked.exists():
        shutil.rmtree(unpacked)


rm_registered = False


@pytest.fixture
def per_run_store():
    from mbf_externals import ExternalAlgorithmStore, change_global_store

    global rm_registered

    base = Path(__file__).parent.parent.parent.parent / "tests" / "run" / "store"
    unpacked = base / "unpacked"
    unpacked.mkdir(exist_ok=True, parents=True)
    (base / "zipped").mkdir(exist_ok=True)
    store = ExternalAlgorithmStore(base / "zipped", unpacked)
    change_global_store(store)
    yield store
    import atexit

    if not rm_registered:
        rm_registered = True
        atexit.register(lambda: shutil.rmtree(base))
