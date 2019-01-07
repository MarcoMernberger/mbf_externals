from .base import Aligner
import pypipegraph as ppg
from pathlib import Path


class Subread(Aligner):
    def __init__(self, parameters, version="_last_used", store=None):
        super().__init__(version, store)
        if not parameters.get("input_type") in ("dna", "rna"):
            raise ValueError("invalid parameters['input_type'], must be dna or rna")

        self.parameters = parameters

    @property
    def name(self):
        return "Subread"

    @property
    def multi_core(self):
        return True

    def build_cmd(self, output_dir, ncores, arguments):
        if (
            not isinstance(arguments, list)
            or len(arguments) < 2
            or arguments[0] != "FROM_SUBREAD"
        ):
            raise ValueError(
                "Please call one of the following functions instead: Subread().align, subread.buildindex"
                + str(arguments)
            )
        if "subread-align" in arguments[1]:
            return arguments[1:] + ["-T", str(ncores)]
        else:
            return arguments[1:]

    def align(
        self, input_fastq, paired_end_filename, index_basename, output_bam_filename
    ):
        if self.parameters["input_type"] == "dna":
            input_type = "1"
        else:
            input_type = "0"
        output_bam_filename = Path(output_bam_filename)
        cmd = [
            "FROM_SUBREAD",
            str(
                self.path
                / f"subread-{self.version}-Linux-x86_64"
                / "bin"
                / "subread-align"
            ),
            "-t",
            input_type,
            "-I",
            "%i" % self.parameters.get("indels_up_to", 5),
            "-B",
            "%i" % self.parameters.get("max_mapping_locations", 1),
            "-i",
            str(Path(index_basename).absolute()),
            "-r",
            str(Path(input_fastq).absolute()),
            "-o",
            str(
                (
                    output_bam_filename.parent / self.version / output_bam_filename.name
                ).absolute()
            ),
        ]
        if paired_end_filename:
            cmd.extend(("-R", str(Path(paired_end_filename).absolute())))
        job = self.run(Path(output_bam_filename).parent, cmd)
        job.depends_on(
            ppg.ParameterInvariant(output_bam_filename, sorted(self.parameters.items()))
        )
        return job

    def build_index(self, fasta_files, gtf_input_filename, output_fileprefix):
        cmd = [
            "FROM_SUBREAD",
            str(
                self.path
                / f"subread-{self.version}-Linux-x86_64"
                / "bin"
                / "subread-buildindex"
            ),
            "-o",
            str(Path(output_fileprefix).absolute()),
        ]
        if not hasattr(fasta_files, "__iter__"):
            fasta_files = [fasta_files]
        cmd.extend([str(Path(x).absolute()) for x in fasta_files])
        job = self.run(Path(output_fileprefix).parent, cmd)
        job.depends_on(ppg.MultiFileInvariant(fasta_files))
        return job

    def fetch_latest_version(self):  # pragma: no cover
        v = "1.6.3"
        if v in self.store.get_available_versions(self.name):
            return
        target_filename = self.store.get_zip_file_path(self.name, v).absolute()
        import requests
        import shutil

        url = f"https://downloads.sourceforge.net/project/subread/subread-{v}/subread-{v}-Linux-x86_64.tar.gz"
        r = requests.get(url, stream=True)
        with open(target_filename, "wb") as op:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, op)
