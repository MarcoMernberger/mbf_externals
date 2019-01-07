from .base import Aligner
import pypipegraph as ppg
from pathlib import Path


class STAR(Aligner):
    def __init__(self, parameters, version="_last_used", store=None):
        super().__init__(version, store)

        self.parameters = parameters

    @property
    def name(self):
        return "STAR"

    @property
    def multi_core(self):
        return True

    def build_cmd(self, output_dir, ncores, arguments):
        if (
            not isinstance(arguments, list)
            or len(arguments) < 2
            or arguments[0] != "FROM_STAR"
        ):
            raise ValueError(
                "Please call one of the following functions instead: Subread().align, subread.buildindex"
                + str(arguments)
            )
        arguments.extend(["--runThreadN", str(ncores)])
        return arguments[1:]

    def align(
        self, input_fastq, paired_end_filename, index_basename, output_bam_filename
    ):
        pass

    def build_index(self, fasta_files, gtf_input_filename, output_fileprefix):
        if isinstance(fasta_files, str):
            fasta_files = [fasta_files]
        if len(fasta_files) > 1:
            raise ValueError("STAR can only build from a single fasta")
        cmd = [
            "FROM_STAR",
            str(self.path / "bin" / "Linux_x86_64_static" / "STAR"),
            "--runMode",
            "genomeGenerate",
            "--genomeDir",
            str(Path(output_fileprefix).absolute()),
            "--sjdbGTFfile",
            str(Path(gtf_input_filename).absolute()),
            "--genomeFastaFiles",
            str(Path(fasta_files[0]).absolute()),
            "--sjdbOverhang",
            "100",
        ]
        job = self.run(Path(output_fileprefix).parent, cmd)
        job.depends_on(ppg.MultiFileInvariant(fasta_files + [gtf_input_filename]))
        return job

    def fetch_latest_version(self):  # pragma: no cover
        v = "2.6.1d"
        if v in self.store.get_available_versions(self.name):
            return
        target_filename = self.store.get_zip_file_path(self.name, v).absolute()
        import requests
        import shutil

        url = "https://github.com/alexdobin/STAR/archive/{v}.zip"
        r = requests.get(url, stream=True)
        with open(target_filename, "wb") as op:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, op)
