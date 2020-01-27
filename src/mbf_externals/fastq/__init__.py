#  import pypipegraph as ppg
#  import time
#  from pathlib import Path
#  from .. import find_code_path
from ..externals import ExternalAlgorithm
from ..util import download_zip_and_turn_into_tar_gzip


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

    @property
    def multi_core(self):
        return False  # fastqc has a threads option - and does not make use of it

    def get_latest_version(self):
        return "0.11.8"

    def fetch_version(self, version, target_filename):  # pragma: no cover

        v = version
        download_zip_and_turn_into_tar_gzip(
            f"https://www.bioinformatics.babraham.ac.uk/projects/fastqc/fastqc_v{v}.zip",
            target_filename,
            ["FastQC/fastqc"],
        )

        print(f"done downloading FASTQC version {v}")
