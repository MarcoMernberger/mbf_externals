from ..externals import ExternalAlgorithm
from abc import abstractmethod


class Aligner(ExternalAlgorithm):
    @abstractmethod
    def align(
        self, input_fastq, paired_end_filename, index_basename, output_bam_filename
    ):
        pass  # pragma: no cover

    @abstractmethod
    def build_index(self, fasta_files, gtf_input_filename, output_prefix):
        pass  # pragma: no cover
