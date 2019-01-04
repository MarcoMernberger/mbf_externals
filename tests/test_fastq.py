from pathlib import Path
import pypipegraph as ppg
from mbf_externals import FASTQC


class TestFASTQC:
    # @pytest.mark.usefixtures("new_pipeline", "global_store")
    def test_quick_run(self, new_pipeline, global_store):
        new_pipeline.quiet = False
        data_path = (Path(__file__).parent / "sample_data").absolute()
        a = FASTQC()
        output_dir = "fastqc_results"
        job = a.run(
            output_dir,
            [data_path / "sample_a", "a.fastq", data_path / "sample_a", "b.fastq.gz"],
        )
        ppg.util.global_pipegraph.run()
        assert Path(job.filenames[0]).exists()
