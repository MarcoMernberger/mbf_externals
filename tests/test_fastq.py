from pathlib import Path
import pypipegraph as ppg
from mbf_externals import FASTQC


class TestFASTQC:
    def test_quick_run(self, new_pipegraph, per_test_store):
        new_pipegraph.quiet = False
        data_path = (Path(__file__).parent / "sample_data").absolute()
        a = FASTQC()
        output_dir = "fastqc_results"
        job = a.run(output_dir, [data_path / "a.fastq", data_path / "b.fastq.gz"])
        ppg.util.global_pipegraph.run()
        assert Path(job.filenames[0]).exists()
        assert Path("fastqc_results/stderr.txt").read_text() == ""
        assert Path("fastqc_results/a_fastqc.html").exists()
