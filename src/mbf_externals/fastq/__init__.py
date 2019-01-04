#  import pypipegraph as ppg
#  import time
#  from pathlib import Path
#  from .. import find_code_path
from ..externals import ExternalAlgorithm


class FASTQC(ExternalAlgorithm):
    @property
    def name(self):
        return "FASTQC"

    def build_cmd(self, output_directory, ncores, arguments):
        input_files = arguments
        return [
            str(self.path / "FastQC" / "fastqc"),
            "-t",
            str(ncores),
            "--noextract",
            "--quiet",
            "-o",
            str(output_directory),
        ] + [str(x) for x in input_files]

    def multi_core(self):
        return True

    def fetch_latest_version(self):  # pragma: no cover
        import requests
        import tempfile
        import shutil
        from pathlib import Path
        import subprocess

        v = "0.11.8"
        if v in self.store.get_available_versions(self.name):
            return
        target_filename = self.store.get_zip_file_path(self.name, v).absolute()
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            url = f"https://www.bioinformatics.babraham.ac.uk/projects/fastqc/fastqc_v{v}.zip"
            print("downloading", url)
            r = requests.get(url, stream=True)
            r.raw.decode_content = True
            zip_file = (tmpdir / "fastqc.zip").open("wb")
            shutil.copyfileobj(r.raw, zip_file)
            zip_file.close()
            import zipfile

            with zipfile.ZipFile(zip_file.name, "r") as zip_ref:
                zip_ref.extractall(tmpdir / "target")
            subprocess.check_call(
                ["chmod", "+x", str(tmpdir / "target" / "FastQC" / "fastqc")]
            )
            subprocess.check_call(
                ["tar", "cf", target_filename, "./"], cwd=tmpdir / "target"
            )
            print(f"done downloading FASTQC version {v}")
