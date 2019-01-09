from ..externals import ExternalAlgorithm
from pathlib import Path
from abc import abstractmethod
import pypipegraph as ppg


class Aligner(ExternalAlgorithm):
    @abstractmethod
    def align_job(
        self,
        input_fastq,
        paired_end_filename,
        index_basename,
        output_bam_filename,
        parameters,
    ):
        pass  # pragma: no cover

    @abstractmethod
    def build_index_func(self, fasta_files, gtf_input_filename, output_prefix):
        pass  # pragma: no cover

    def build_index_job(self, fasta_files, gtf_input_filename, output_fileprefix):
        output_directory = Path(output_fileprefix)
        output_directory.mkdir(parents=True, exist_ok=True)
        sentinel = output_directory / "sentinel.txt"
        job = ppg.FileGeneratingJob(
            sentinel,
            self.build_index_func(fasta_files, gtf_input_filename, output_directory),
        ).depends_on(
            ppg.FileChecksumInvariant(
                self.store.get_zip_file_path(self.name, self.version)
            )
        )
        if self.multi_core:
            job.cores_needed = -1
        return job

    def build_index(self, fasta_files, gtf_input_filename, output_fileprefix):
        output_fileprefix = Path(output_fileprefix)
        output_fileprefix.mkdir(parents=True, exist_ok=True)
        func = self.build_index_func(fasta_files, gtf_input_filename, output_fileprefix)
        func()

    def get_index_version_range(self):  # pragma: no cover
        return None, None
