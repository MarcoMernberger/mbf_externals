from .externals import ExternalAlgorithm, ExternalAlgorithmStore, change_global_store
from .fastq import FASTQC
from .prebuild import PrebuildManager, change_global_manager
from . import aligners
from pathlib import Path
import os


def create_defaults():

    if "MBF_EXTERNAL_PREBUILD_PATH" in os.environ:
        hostname = os.environ["MBF_EXTERNAL_HOSTNAME"]
        store_base = (
            Path(os.environ["MBF_EXTERNAL_PREBUILD_PATH"]) / hostname / "mbf_store"
        )
        prebuild_path = Path(os.environ["MBF_EXTERNAL_PREBUILD_PATH"])

    elif "VIRTUAL_ENV" in os.environ:
        import socket

        store_base = Path(os.environ["VIRTUAL_ENV"]) / "mbf_store"
        prebuild_path = (Path(".") / "prebuild").absolute()
        hostname = socket.gethostname()
    else:
        # print("No defaults for mbf_externals possible")
        return

    zipped = store_base / "zip"
    unpacked = store_base / "unpack"
    store_base.mkdir(exist_ok=True)
    zipped.mkdir(exist_ok=True)
    unpacked.mkdir(exist_ok=True)
    change_global_store(ExternalAlgorithmStore(zipped, unpacked))
    change_global_manager(PrebuildManager(prebuild_path, hostname))


create_defaults()

__all__ = [
    ExternalAlgorithm,
    ExternalAlgorithmStore,
    FASTQC,
    change_global_store,
    PrebuildManager,
    aligners,
    create_defaults(),
]
