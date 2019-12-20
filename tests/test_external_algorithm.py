import subprocess
from pathlib import Path
import pypipegraph as ppg
import pytest
from mbf_externals import ExternalAlgorithm
from mbf_externals.util import Version
import tempfile


class DummyAlgorithm(ExternalAlgorithm):
    @property
    def name(self):
        return "dummy"

    def build_cmd(self, output_directory, ncores, arguments):
        return [self.path / "dummy.sh"]

    def multi_core(self):
        return True

    def get_latest_version(self):
        return "0.10"

    def fetch_version(self, version, target_filename):
        if version == "0.2nsv":
            raise ValueError("no such version")
        else:
            return super().fetch_version(version, target_filename)(self)


class WhateverAlgorithm(ExternalAlgorithm):
    @property
    def name(self):
        return "whatever"

    def build_cmd(self, output_directory, ncores, return_code):
        return [self.path / "whatever.sh", str(return_code)]

    def get_latest_version(self):
        return "0.1"

    def fetch_version(self, version, target_filename):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            (tmpdir / "whatever.sh").write_text('#!/bin/bash\necho "was $1"\n exit $1')
            subprocess.check_call(["chmod", "+x", str(tmpdir / "whatever.sh")])
            subprocess.check_call(["tar", "cf", target_filename, "./"], cwd=tmpdir)


class SelfFetchingAlgorithm(ExternalAlgorithm):
    @property
    def name(self):
        return "fetchme"

    def build_cmd(self, output_directory, ncores, arguments):
        return [self.path / "fetchme.sh"]

    def get_latest_version(self):
        return "funny_funny__version"

    def fetch_version(self, version, target_filename):
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            (tmpdir / "fetchme.sh").write_text('#!/bin/bash\necho "fetched"')
            subprocess.check_call(["chmod", "+x", str(tmpdir / "fetchme.sh")])
            subprocess.check_call(["tar", "cf", target_filename, "./"], cwd=tmpdir)


random_counter = 0


class SelfFetchingAlgorithmRandomFileEachTime(ExternalAlgorithm):
    @property
    def name(self):
        return "fetchme"

    def build_cmd(self, output_directory, ncores, arguments):
        return [self.path / "fetchme.sh"]

    def get_latest_version(self):
        return "0.1"

    def fetch_version(self, version, target_filename):
        import tempfile

        global random_counter

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            (tmpdir / "fetchme.sh").write_text(
                '#!/bin/bash\necho "fetched"' + str(random_counter)
            )
            random_counter += 1
            subprocess.check_call(["chmod", "+x", str(tmpdir / "fetchme.sh")])
            subprocess.check_call(["tar", "cf", target_filename, "./"], cwd=tmpdir)


class TestExternalStore:
    def test_get_versions(self, local_store, new_pipegraph):
        assert local_store.get_available_versions("dummy") == ["0.1", "0.2", "0.10"]
        assert local_store.get_available_versions("another_dummy") == ["0.2"]

    def test_unpack_version(self, local_store, new_pipegraph):
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

    def test_algo_get_latest(self, new_pipegraph, local_store):
        algo = DummyAlgorithm(version="_latest")
        assert algo.version == "0.10"
        job = algo.run(new_pipegraph.result_dir / "dummy_output")
        assert job.cores_needed == -1
        ppg.util.global_pipegraph.run()
        assert Path(job.filenames[0]).exists()
        assert (
            Path(job.filenames[0]).parent / "stdout.txt"
        ).read_text() == "hello world10\n"
        assert (Path(job.filenames[0]).parent / "stderr.txt").read_text() == ""

    def test_algo_get_auto_from_scratch(self, new_pipegraph, local_store):
        algo = DummyAlgorithm(version="_last_used")
        assert algo.version == "0.10"
        assert Path(".mbf_external_versions").read_text() == "dummy==0.10\n"

    def test_algo_get_auto_from_after_pull(self, new_pipegraph, local_store):
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

    def test_algo_get_specific(self, new_pipegraph, local_store):
        algo = DummyAlgorithm("0.2")
        assert algo.version == "0.2"

    def test_algo_get_non_existant(self, new_pipegraph, per_test_store):
        # per test store - otherwise downloads are off
        with pytest.raises(ValueError):
            DummyAlgorithm("0.2nsv")

    def test_passing_arguments(self, new_pipegraph, per_test_store):
        algo = WhateverAlgorithm()
        job = algo.run(new_pipegraph.result_dir / "whatever_output", 0)
        assert job.cores_needed == 1
        ppg.util.global_pipegraph.run()
        assert Path(job.filenames[0]).exists()
        assert (Path(job.filenames[0]).parent / "stdout.txt").read_text() == "was 0\n"
        assert (Path(job.filenames[0]).parent / "stderr.txt").read_text() == ""
        assert (Path(job.filenames[0]).parent / "cmd.txt").read_text() == repr(
            [
                str(Path("../../../tests/run/TestExternalStore.test_passing_arguments/store/unpacked/whatever/0.1/whatever.sh").resolve()),
                "0",
            ]
        )

    def test_passing_arguments_and_returncode_issues(
        self, new_pipegraph, per_test_store
    ):
        algo = WhateverAlgorithm()
        job = algo.run(new_pipegraph.result_dir / "mhatever_output", 1)
        with pytest.raises(ppg.RuntimeError):
            ppg.util.global_pipegraph.run()
        assert not Path(job.filenames[0]).exists()
        assert (Path(job.filenames[0]).parent / "stdout.txt").read_text() == "was 1\n"
        assert (Path(job.filenames[0]).parent / "stderr.txt").read_text() == ""

    def test_fetching(self, per_test_store, new_pipegraph):
        tf = per_test_store.zip_path / "fetchme__funny_funny__version.tar.gz"
        if tf.exists():
            tf.unlink()
        assert len(per_test_store.get_available_versions("fetchme")) == 0
        SelfFetchingAlgorithm()
        assert len(per_test_store.get_available_versions("fetchme")) == 1
        assert (
            per_test_store.get_available_versions("fetchme")[0]
            == "funny_funny__version"
        )

    def test_hashsum_checking_against_other_machines(self, new_pipegraph):
        from mbf_externals import (
            ExternalAlgorithmStore,
            change_global_store,
        )
        from mbf_externals.externals import DownloadDiscrepancyException

        Path("store1/zipped").mkdir(parents=True)
        Path("store1/extracted").mkdir(parents=True)
        Path("store2/zipped").mkdir(parents=True)
        Path("store2/extracted").mkdir(parents=True)

        store = ExternalAlgorithmStore("store1/zipped", "store1/extracted")
        change_global_store(store)
        SelfFetchingAlgorithmRandomFileEachTime()  # which 'downloads' one file.
        store = ExternalAlgorithmStore("store2/zipped", "store2/extracted")
        change_global_store(store)
        with pytest.raises(DownloadDiscrepancyException):
            SelfFetchingAlgorithmRandomFileEachTime()  # which 'downloads' a different file and explodes


class TestUtils:
    def test_get_page(self):
        from mbf_externals.util import get_page

        assert "gtf" in get_page("http://ftp.ensembl.org/pub/release-77/")
        assert "gtf" in get_page("ftp://ftp.ensembl.org/pub/release-77/")
        assert "gtf" in get_page("ftp://ftp.ensembl.org/pub/release-77/README")
        with pytest.raises(ValueError):
            get_page("ftp://ftp.ensembl.org/pub/release-77/doesnotexist")

    def test_download_file_and_gunzip(self, new_pipegraph):
        from mbf_externals.util import download_file_and_gunzip

        download_file_and_gunzip(
            "http://ftp.ensembl.org/pub/release-77/mysql/ailuropoda_melanoleuca_core_77_1/map.txt.gz",
            "map.txt",
        )
        assert Path("map.txt").read_text() == ""

    def test_download_file_with_filename_raises(self):
        from mbf_externals.util import download_file

        with pytest.raises(ValueError):
            download_file("http://ftp.ensembl.org", "out.file")

    def test_compare_versions(self):
        assert Version("") == ""
        assert Version("") == Version("")
        assert Version("0.1") == Version("0.1")
        assert Version("0.2") > "0.1"
        assert Version("0.2") < "0.3"
        assert Version("1.5.0") < "1.6.0"
        assert Version("1.4.3") < "1.5.0"
        assert Version("1.4.3-p1") < "1.5.99"
        assert not (Version("0.4") > "0.4")
        assert not (Version("0.4") < "0.4")
        assert Version("0.4") >= "0.4"
        assert Version("0.4") <= "0.4"
        assert Version("0.4") < "0.5"
        assert Version("0.4") < "0.6"
        assert Version("1.5.0") < "1.6"
        assert str(Version("1.5")) == "1.5"
        assert repr(Version("1.5")) == 'Version("1.5")'
        assert Version("1.5.0") < Version("1.6")
        assert Version("1.6.0") > Version("1.5.99.shu")


def test_create_defaults(new_pipegraph):
    import os
    import socket
    import shutil
    from mbf_externals import create_defaults, get_global_manager, get_global_store

    org_environ = os.environ.copy()
    try:
        for k in "VIRTUAL_ENV", "MBF_EXTERNAL_PREBUILD_PATH", "MBF_EXTERNAL_HOSTNAME":
            if k in os.environ:
                del os.environ[k]
            os.environ["VIRTUAL_ENV"] = "fakevirt"
        Path("fakevirt").mkdir()
        create_defaults()
        gs = get_global_store()
        assert gs.zip_path == Path("fakevirt") / "mbf_store" / "zip"
        assert gs.unpack_path == Path("fakevirt") / "mbf_store" / "unpack"
        gm = get_global_manager()
        assert gm.prebuilt_path.absolute() == (Path(".") / "prebuilt").absolute()
        assert (Path(".") / "prebuilt" / socket.gethostname()).exists()

        del os.environ["VIRTUAL_ENV"]
        shutil.rmtree("fakevirt")
        Path("fakevirt").mkdir()
        os.environ["MBF_EXTERNAL_PREBUILD_PATH"] = "fakevirt"
        with pytest.raises(KeyError):
            create_defaults()
        os.environ["MBF_EXTERNAL_HOSTNAME"] = "myname"
        with pytest.raises(ValueError):
            create_defaults()
        Path("fakevirt/myname").mkdir()
        create_defaults()
        gs = get_global_store()
        assert gs.zip_path == Path("fakevirt") / "myname" / "mbf_store" / "zip"
        assert gs.unpack_path == Path("fakevirt") / "myname" / "mbf_store" / "unpack"
        gm = get_global_manager()
        assert gm.prebuilt_path == Path("fakevirt")
        shutil.rmtree("fakevirt")
        del os.environ["MBF_EXTERNAL_PREBUILD_PATH"]
        del os.environ["MBF_EXTERNAL_HOSTNAME"]

        create_defaults()
        assert get_global_manager() is None
        assert get_global_store() is None

    finally:
        os.environ = org_environ
