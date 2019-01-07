from pathlib import Path
import pytest
from mbf_externals.aligners.subread import Subread


class TestSubread:
    def test_build_and_align(self, new_pipeline, global_store):
        new_pipeline.quiet = False
        s = Subread({"input_type": "dna"})
        s.fetch_latest_version()
        data_path = Path(__file__).parent / "sample_data"
        index_name = "subread_index_dir/srf"
        build_job = s.build_index([data_path / "genome.fasta"], None, index_name)
        align_job = s.align(data_path / "sample.fastq", None, index_name, "out/out.bam")
        align_job.depends_on(build_job)
        new_pipeline.run()
        assert (Path("out") / s.version / "out.bam").exists()

    def test_raises_on_invalid_input_type(self):
        with pytest.raises(TypeError):
            Subread()
        with pytest.raises(ValueError):
            Subread(parameters={"input_type": "sha"})

    def test_cant_call_subread_run(self, new_pipeline):
        s = Subread({"input_type": "dna"})
        s.run("out", None)
        with pytest.raises(ValueError):
            new_pipeline.run()

    def test_cant_call_subread_run2(self, new_pipeline):
        s = Subread({"input_type": "dna"})
        s.run("out", ["subread-align", "something"])
        with pytest.raises(ValueError):
            new_pipeline.run()

    def test_build_and_align_paired_end(self, new_pipeline, global_store):
        new_pipeline.quiet = False
        s = Subread({"input_type": "rna"})
        s.fetch_latest_version()
        data_path = Path(__file__).parent / "sample_data"
        index_name = "subread_index_dir/srf"
        build_job = s.build_index(data_path / "genome.fasta", None, index_name)
        align_job = s.align(
            data_path / "sample_R1_.fastq",
            data_path / "sample_R2_.fastq",
            index_name,
            "out/out.bam",
        )
        align_job.depends_on(build_job)
        new_pipeline.run()
        assert (Path("out") / s.version / "out.bam").exists()
