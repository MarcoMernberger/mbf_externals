#!/usr/bin/python3
"""Find mbf_store zips that are non identical on the different machines.
Very ZTI specific

"""
from pypipegraph.util import checksum_file
import json
import subprocess
from pathlib import Path


machines = json.loads(
    subprocess.check_output(["ffs.py", "list_targets"]).decode("utf-8")
)

found = {}

for m in machines:
    p = Path("/") / m / "ffs" / "prebuild/externals"
    if not p.exists():
        if not (Path("/") / m).exists():
            print(f"{m} not mounted! missing from dataset")
        continue

    for sm in p.glob("*"):
        if sm.is_dir():
            store_path = sm / "mbf_store" / "zip"
            if store_path.exists():
                for zip in store_path.glob("*.tar.gz"):
                    hash = checksum_file(zip)
                    if not zip.name in found:
                        found[zip.name] = {}
                    if not hash in found[zip.name]:
                        found[zip.name][hash] = [str(zip)]

any_found = False
for name, hashes in found.items():
    if len(hashes) > 1:
        any_found = True
        print("Multiple hashes for", name)
        for hash, files in hashes.items():
            print(f"\t{hash}")
            for f in files:
                print(f"\t\t{f}")
if not any_found:
    print("No discrepancies found.")
