from .base import Aligner
import pypipegraph as ppg
from pathlib import Path
from ..util import download_file


class Bowtie(Aligner):
    def __init__(self, version="_last_used", store=None):
        super().__init__(version, store)

    @property
    def name(self):
        return "Bowtie"

    @property
    def multi_core(self):
        return True

    def _aligner_build_cmd(self, output_dir, ncores, arguments):
        return arguments + ["--threads", ncores]

    def align_job(
        self,
        input_fastq,
        paired_end_filename,
        index_basename,
        output_bam_filename,
        parameters,
    ):
        cmd = [
            "FROM_ALIGNER",
            self.path / f"bowtie-{self.version}-linux-x86_64" / "bowtie",
            (Path(index_basename) / 'bowtie_index').absolute(),
            "-S",
        ]
        if paired_end_filename:
            cmd.extend(
                [
                    "-1",
                    Path(paired_end_filename).absolute(),
                    "-2",
                    Path(input_fastq).absolute(),
                ]
            )
        else:
            cmd.extend([Path(input_fastq).absolute()])
        cmd.append(str(Path(output_bam_filename).absolute()) + ".sam")
        if not "--seed" in parameters:
            parameters["--seed"] = "123123"
        for k, v in parameters.items():
            cmd.append(k)
            cmd.append(str(v))

        def sam_to_bam():
            import pysam

            infile = pysam.AlignmentFile(str(output_bam_filename) + ".sam", "r")
            outfile = pysam.AlignmentFile(
                str(output_bam_filename), "wb", template=infile
            )
            for s in infile:
                outfile.write(s)

        job = self.run(
            Path(output_bam_filename).parent,
            cmd,
            cwd=Path(output_bam_filename).parent,
            call_afterwards=sam_to_bam,
        )
        job.depends_on(
            ppg.ParameterInvariant(output_bam_filename, sorted(parameters.items()))
        )
        return job

    def build_index_func(self, fasta_files, gtf_input_filename, output_fileprefix):
        if isinstance(fasta_files, (str, Path)):
            fasta_files = [fasta_files]
        if len(fasta_files) > 1:
            raise ValueError("Bowtie can only build from a single fasta")
        cmd = [
            "FROM_ALIGNER",
            self.path / f"bowtie-{self.version}-linux-x86_64" / "bowtie-build",
            ",".join([str(Path(x).absolute()) for x in fasta_files]),
            (Path(output_fileprefix) / 'bowtie_index').absolute(),
            "--seed",
            "123123",
        ]
        return self.get_run_func(output_fileprefix, cmd, cwd=output_fileprefix)

    def fetch_latest_version(self):  # pragma: no cover
        import tempfile
        import subprocess

        version = "1.2.2"
        url = f"https://downloads.sourceforge.net/project/bowtie-bio/bowtie/{version}/bowtie-{version}-linux-x86_64.zip"
        print(url)
        if version in self.store.get_available_versions(self.name):
            return
        target_filename = self.store.get_zip_file_path(self.name, version).absolute()
        tf = tempfile.NamedTemporaryFile("wb", suffix=".zip")
        download_file(url, tf)
        tf.flush()
        td = tempfile.TemporaryDirectory()
        subprocess.check_call(["unzip", str(Path(tf.name).absolute())], cwd=td.name)
        subprocess.check_call(["tar", "cf", str(target_filename), "."], cwd=td.name)
