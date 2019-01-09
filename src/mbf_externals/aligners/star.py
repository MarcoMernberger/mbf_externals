from .base import Aligner
import pypipegraph as ppg
from pathlib import Path
from ..util import download_file


class STAR(Aligner):
    def __init__(self, version="_last_used", store=None):
        super().__init__(version, store)

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

    def align_job(
        self,
        input_fastq,
        paired_end_filename,
        index_basename,
        output_bam_filename,
        parameters,
    ):
        cmd = [
            "FROM_STAR",
            str(
                self.path
                / f"STAR-{self.version}"
                / "bin"
                / "Linux_x86_64_static"
                / "STAR"
            ),
            "--genomeDir",
            Path(index_basename).absolute(),
            "--genomeLoad",
            "NoSharedMemory",
            "--readFilesIn",
        ]
        if paired_end_filename:
            cmd.extend(
                [
                    '"%s"' % Path(paired_end_filename).absolute(),
                    '"%s"' % Path(input_fastq).absolute(),
                ]
            )
        else:
            cmd.extend([Path(input_fastq).absolute()])
        cmd.extend(["--outSAMtype", "BAM", "SortedByCoordinate"])
        for k, v in parameters.items():
            cmd.append(k)
            cmd.append(str(v))

        def rename_after_alignment():
            ob = Path(output_bam_filename)
            (ob.parent / "Aligned.sortedByCoord.out.bam").rename(ob.parent / ob.name)

        job = self.run(
            Path(output_bam_filename).parent,
            cmd,
            cwd=Path(output_bam_filename).parent,
            call_afterwards=rename_after_alignment,
        )
        job.depends_on(
            ppg.ParameterInvariant(output_bam_filename, sorted(parameters.items()))
        )
        return job

    def build_index_func(self, fasta_files, gtf_input_filename, output_fileprefix):
        if isinstance(fasta_files, (str, Path)):
            fasta_files = [fasta_files]
        if len(fasta_files) > 1:
            raise ValueError("STAR can only build from a single fasta")
        if gtf_input_filename is None:
            raise ValueError(
                "STAR needs a gtf input file to calculate splice junctions"
            )
        cmd = [
            "FROM_STAR",
            self.path / f"STAR-{self.version}" / "bin" / "Linux_x86_64_static" / "STAR",
            "--runMode",
            "genomeGenerate",
            "--genomeDir",
            Path(output_fileprefix).absolute(),
            "--sjdbGTFfile",
            Path(gtf_input_filename).absolute(),
            "--genomeFastaFiles",
            Path(fasta_files[0]).absolute(),
            "--sjdbOverhang",
            "100",
        ]
        return self.get_run_func(output_fileprefix, cmd, cwd=output_fileprefix)

    def fetch_latest_version(self):  # pragma: no cover
        v = "2.6.1d"
        if v in self.store.get_available_versions(self.name):
            return
        target_filename = self.store.get_zip_file_path(self.name, v).absolute()
        url = f"https://github.com/alexdobin/STAR/archive/{v}.tar.gz"
        with open(target_filename, "wb") as op:
            download_file(url, op)
