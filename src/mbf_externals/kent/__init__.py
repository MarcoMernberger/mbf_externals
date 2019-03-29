# and   import pypipegraph as ppg
#  import time
#  from pathlib import Path
#  from .. import find_code_path
from ..externals import ExternalAlgorithm
from ..util import download_file


class LiftOver(ExternalAlgorithm):
    @property
    def name(self):
        return "LiftOver"

    def build_cmd(self, output_directory, ncores, arguments):  # pragma: no cover
        """Arguments = oldFile, map.chain, newFile"""
        return [str(self.path / "liftOver")] + arguments

    @property
    def multi_core(self):  # pragma: no cover
        return False

    def fetch_latest_version(self):  # pragma: no cover
        import tempfile
        from pathlib import Path
        import subprocess

        v = "0.1"
        if v in self.store.get_available_versions(self.name):
            return
        target_filename = self.store.get_zip_file_path(self.name, v).absolute()
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            url = "http://hgdownload.cse.ucsc.edu/admin/exe/linux.x86_64/liftOver"
            with (tmpdir / "liftOver").open("wb") as zip_file:
                download_file(url, zip_file)
                subprocess.check_call(["chmod", "+x", str(tmpdir / "liftOver")])

            subprocess.check_call(
                ["tar", "cf", target_filename, "./liftOver"], cwd=tmpdir
            )
            print(f"done downloading liftover version {v}")
