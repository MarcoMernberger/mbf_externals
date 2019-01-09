#!/usr/bin/python3
import sys
from pathlib import Path
import os


def all_subclasses(cls):
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)]
    )


def print_usage(msg=""):
    print("fetch_all.py <dir>")
    print(
        "Fetches latest versions of all algorithms (if not already present) into <dir>"
    )
    print("to use as zipped dir in ExternalAlgorithmStore")
    print("If no <dir> is given, fetch to VIRTUAL_ENV/mbf_store/zip")
    if msg:
        print(msg)
    sys.exit(1)


if __name__ == "__main__":
    try:
        target_dir = Path(sys.argv[1])
    except IndexError:
        if "VIRTUAL_ENV" in os.environ:
            zipped = Path(os.environ["VIRTUAL_ENV"]) / "mbf_store" / "zip"
            target_dir = zipped
            target_dir.mkdir(exist_ok=True, parents=True)
        else:
            print_usage("NO VIRTUAL_ENV active")
    if not (target_dir.exists() and target_dir.is_dir()):
        print_usage("<dir> was not a directory")
    print("Downloading into", str(target_dir.absolute()))
    sys.path.insert(0, str((Path(__file__).parent.parent / "src").absolute()))
    import mbf_externals

    store = mbf_externals.ExternalAlgorithmStore(target_dir, "doesnotexist")
    mbf_externals.change_global_store(store)
    for sc in all_subclasses(mbf_externals.ExternalAlgorithm):
        if hasattr(sc, "fetch_latest_version"):
            try:
                x = sc(version="_fetching")
                print("fetching", x.name)
                x.fetch_latest_version()
            except TypeError as e:
                if "abstract class" in str(e):
                    pass
                else:
                    raise
