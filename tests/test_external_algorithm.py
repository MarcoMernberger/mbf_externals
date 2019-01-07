import subprocess
from pathlib import Path
import pypipegraph as ppg
import pytest
from mbf_externals import ExternalAlgorithm


class DummyAlgorithm(ExternalAlgorithm):
    @property
    def name(self):
        return "dummy"

    def build_cmd(self, output_directory, ncores, arguments):
        return [self.path / "dummy.sh"]

    def multi_core(self):
        return True


class WhateverAlgorithm(ExternalAlgorithm):
    @property
    def name(self):
        return "whatever"

    def build_cmd(self, output_directory, ncores, return_code):
        return [self.path / "whatever.sh", str(return_code)]


class SelfFetchingAlgorithm(ExternalAlgorithm):
    @property
    def name(self):
        return "fetchme"

    def build_cmd(self, output_directory, ncores, arguments):
        return [self.path / "fetchme.sh"]

    def fetch_latest_version(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            (tmpdir / "fetchme.sh").write_text('#!/bin/bash\necho "fetched"')
            subprocess.check_call(["chmod", "+x", str(tmpdir / "fetchme.sh")])
            v = "funny_funny__version"
            target_filename = self.store.get_zip_file_path(self.name, v).absolute()
            subprocess.check_call(["tar", "cf", target_filename, "./"], cwd=tmpdir)


class TestExternalStore:
    def test_get_versions(self, local_store, new_pipeline):
        assert local_store.get_available_versions("dummy") == ["0.1", "0.2", "0.10"]
        assert local_store.get_available_versions("another_dummy") == ["0.2"]

    def test_unpack_version(self, local_store, new_pipeline):
        local_store.unpack_version("dummy", "0.1")
        assert (local_store.unpack_path / "dummy" / "0.1" / "unpack_done.txt").exists()
        assert (local_store.unpack_path / "dummy" / "0.1" / "dummy.sh").exists()

        local_store.unpack_version("dummy", "0.2")
        assert (local_store.unpack_path / "dummy" / "0.2" / "unpack_done.txt").exists()
        assert (local_store.unpack_path / "dummy" / "0.2" / "dummy.sh").exists()
        assert (
            subprocess.check_output(
                str(local_store.unpack_path / "dummy" / "0.2" / "dummy.sh")
            )
            == b"hello world2\n"
        )

        assert not (local_store.unpack_path / "dummy" / "0.10").exists()
        local_store.unpack_version("dummy", "0.10")
        assert (local_store.unpack_path / "dummy" / "0.10" / "unpack_done.txt").exists()
        assert (local_store.unpack_path / "dummy" / "0.10" / "dummy.sh").exists()
        assert (
            subprocess.check_output(
                str(local_store.unpack_path / "dummy" / "0.10" / "dummy.sh")
            )
            == b"hello world10\n"
        )

        with pytest.raises(ValueError):
            local_store.unpack_version("dummy", "0.15")

        with pytest.raises(ValueError):
            local_store.unpack_version("nosuchalgorithm", "0.15")

    def test_algo_get_latest(self, new_pipeline):
        algo = DummyAlgorithm(version="_latest")
        assert algo.version == "0.10"
        job = algo.run(new_pipeline.result_dir / "dummy_output")
        assert job.cores_needed == -1
        ppg.util.global_pipegraph.run()
        assert Path(job.filenames[0]).exists()
        assert (
            Path(job.filenames[0]).parent / "stdout.txt"
        ).read_text() == "hello world10\n"
        assert (Path(job.filenames[0]).parent / "stderr.txt").read_text() == ""

    def test_algo_get_auto_from_scratch(self, new_pipeline):
        algo = DummyAlgorithm(version="_last_used")
        assert algo.version == "0.10"
        assert Path(".mbf_external_versions").read_text() == "dummy==0.10\n"

    def test_algo_get_auto_from_after_pull(self, new_pipeline):
        algo = DummyAlgorithm(version="0.2")
        assert algo.version == "0.2"
        assert Path(".mbf_external_versions").read_text() == "dummy==0.2\n"
        algo = DummyAlgorithm(version="_last_used")
        assert algo.version == "0.2"
        assert Path(".mbf_external_versions").read_text() == "dummy==0.2\n"
        algo = DummyAlgorithm(version="0.10")
        assert algo.version == "0.10"
        assert Path(".mbf_external_versions").read_text() == "dummy==0.10\n"
        algo = WhateverAlgorithm()
        assert (
            Path(".mbf_external_versions").read_text() == "dummy==0.10\nwhatever==0.1\n"
        )

    def test_algo_get_specific(self, new_pipeline):
        algo = DummyAlgorithm("0.2")
        assert algo.version == "0.2"

    def test_algo_get_non_existant(self, new_pipeline):
        with pytest.raises(ValueError):
            DummyAlgorithm("0.2nsv")

    def test_passing_arguments(self, new_pipeline):
        algo = WhateverAlgorithm()
        job = algo.run(new_pipeline.result_dir / "whatever_output", 0)
        assert job.cores_needed == 1
        ppg.util.global_pipegraph.run()
        assert Path(job.filenames[0]).exists()
        assert (Path(job.filenames[0]).parent / "stdout.txt").read_text() == "was 0\n"
        assert (Path(job.filenames[0]).parent / "stderr.txt").read_text() == ""
        assert (Path(job.filenames[0]).parent / "cmd.txt").read_text() == repr(
            [
                str(Path("../../../tests/unpacked/whatever/0.1/whatever.sh").resolve()),
                "0",
            ]
        )

    def test_passing_arguments_and_returncode_issues(self, new_pipeline):
        algo = WhateverAlgorithm()
        job = algo.run(new_pipeline.result_dir / "whatever_output", 1)
        with pytest.raises(ppg.RuntimeError):
            ppg.util.global_pipegraph.run()
        assert not Path(job.filenames[0]).exists()
        assert (Path(job.filenames[0]).parent / "stdout.txt").read_text() == "was 1\n"
        assert (Path(job.filenames[0]).parent / "stderr.txt").read_text() == ""

    def test_fetching(self, local_store, new_pipeline):
        tf = local_store.zip_path / "fetchme__funny_funny__version.tar.gz"
        if tf.exists():
            tf.unlink()
        assert len(local_store.get_available_versions("fetchme")) == 0
        SelfFetchingAlgorithm()
        assert len(local_store.get_available_versions("fetchme")) == 1
        assert (
            local_store.get_available_versions("fetchme")[0] == "funny_funny__version"
        )
