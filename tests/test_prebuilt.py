import pytest
import pypipegraph as ppg
from pathlib import Path
from mbf_externals import PrebuildManager
from mbf_externals.util import UpstreamChangedError


class TestPrebuilt:
    def test_simple(self, new_pipeline):
        new_pipeline.quiet = False
        Path("prebuilt").mkdir()
        mgr = PrebuildManager("prebuilt", "test_host")
        input_files = [Path("one"), Path("two")]
        input_files[0].write_text("hello")
        input_files[1].write_text("world")
        output_files = [Path("outA")]
        count_file = Path("count")
        count_file.write_text("0")

        def calc(output_path):
            t = "\n".join([i.read_text() for i in input_files])
            c = int(count_file.read_text())
            (output_path / output_files[0]).write_text(t + str(c))
            count_file.write_text(str(c + 1))

        jobA = mgr.prebuild("dummy", "0.1", input_files, output_files, calc)
        ppg.FileGeneratingJob(
            "shu",
            lambda: Path("shu").write_text(Path(jobA.find_file("outA")).read_text()),
        ).depends_on(jobA)
        ppg.util.global_pipegraph.run()
        assert Path("prebuilt/test_host/dummy/0.1/outA").read_text() == "hello\nworld0"
        assert Path("shu").read_text() == "hello\nworld0"

        # no rerunning.
        new_pipeline = new_pipeline.new_pipeline()
        jobA = mgr.prebuild("dummy", "0.1", input_files, output_files, calc)
        ppg.FileGeneratingJob(
            "shu",
            lambda: Path("shu").write_text(Path(jobA.find_file("outA")).read_text()),
        ).depends_on(jobA)
        ppg.util.global_pipegraph.run()
        assert Path("prebuilt/test_host/dummy/0.1/outA").read_text() == "hello\nworld0"
        assert Path("shu").read_text() == "hello\nworld0"

        # no rerunning, getting from second path...
        new_pipeline = new_pipeline.new_pipeline()
        mgr = PrebuildManager("prebuilt", "test_host2")
        assert not Path("prebuilt/test_host2/dummy/0.1/").exists()
        jobA = mgr.prebuild("dummy", "0.1", input_files, output_files, calc)
        assert not Path("prebuilt/test_host2/dummy/0.1/").exists()
        ppg.FileGeneratingJob(
            "shu2",
            lambda: Path("shu2").write_text(Path(jobA.find_file("outA")).read_text()),
        ).depends_on(jobA)
        ppg.util.global_pipegraph.run()
        assert not Path("prebuilt/test_host2/dummy/0.1/").exists()
        assert Path("prebuilt/test_host/dummy/0.1/outA").read_text() == "hello\nworld0"
        assert Path("shu2").read_text() == "hello\nworld0"

        # changes to the input files, same machine -> explode.
        new_pipeline.new_pipeline()
        mgr = PrebuildManager("prebuilt", "test_host")
        input_files[1].write_text("world!")
        jobA = mgr.prebuild("dummy", "0.1", input_files, output_files, calc)
        ppg.FileGeneratingJob(
            "shu3",
            lambda: Path("shu3").write_text(Path(jobA.find_file("outA")).read_text()),
        ).depends_on(jobA)
        with pytest.raises(UpstreamChangedError):
            ppg.util.global_pipegraph.run()
        assert not Path("shu3").exists()
        assert Path("prebuilt/test_host/dummy/0.1/outA").read_text() == "hello\nworld0"

        # changes to the input files, different machine -> explode.
        new_pipeline.new_pipeline()
        mgr = PrebuildManager("prebuilt", "test_host2")
        input_files[1].write_text("world!")
        jobA = mgr.prebuild("dummy", "0.1", input_files, output_files, calc)
        ppg.FileGeneratingJob(
            "shu3",
            lambda: Path("shu3").write_text(Path(jobA.find_file("outA")).read_text()),
        ).depends_on(jobA)
        with pytest.raises(UpstreamChangedError):
            ppg.util.global_pipegraph.run()
        assert not Path("shu3").exists()
        assert Path("prebuilt/test_host/dummy/0.1/outA").read_text() == "hello\nworld0"

        # but new version is ok...
        new_pipeline.new_pipeline()
        mgr = PrebuildManager("prebuilt", "test_host")
        jobA = mgr.prebuild("dummy", "0.2", input_files, output_files, calc)
        ppg.FileGeneratingJob(
            "shu3",
            lambda: Path("shu3").write_text(Path(jobA.find_file("outA")).read_text()),
        ).depends_on(jobA)
        ppg.util.global_pipegraph.run()
        assert Path("prebuilt/test_host/dummy/0.2/outA").read_text() == "hello\nworld!1"
        assert Path("shu3").read_text() == "hello\nworld!1"

        # request same files with minimum_acceptable_version-> no rebuild...
        new_pipeline.new_pipeline()
        jobA = mgr.prebuild(
            "dummy",
            "0.3",
            input_files,
            output_files,
            calc,
            minimum_acceptable_version="0.1",
        )
        ppg.FileGeneratingJob(
            "shu4",
            lambda: Path("shu4").write_text(Path(jobA.find_file("outA")).read_text()),
        ).depends_on(jobA)
        ppg.util.global_pipegraph.run()
        assert Path("shu4").read_text() == "hello\nworld!1"

        # but with minimum = version, and only older available -> rebuild
        new_pipeline.new_pipeline()
        jobA = mgr.prebuild(
            "dummy",
            "0.3",
            input_files,
            output_files,
            calc,
            minimum_acceptable_version="0.3",
        )
        ppg.FileGeneratingJob(
            "shu5",
            lambda: Path("shu5").write_text(Path(jobA.find_file("outA")).read_text()),
        ).depends_on(jobA)
        ppg.util.global_pipegraph.run()
        assert Path("shu5").read_text() == "hello\nworld!2"

        # changing the function leads to an exception
        new_pipeline.new_pipeline()
        mgr = PrebuildManager("prebuilt", "test_host")

        def calc2(output_path):
            t = "\n".join([i.read_text() for i in input_files])
            c = int(count_file.read_text())
            (output_path / output_files[0]).write_text(t + str(c))
            count_file.write_text(str(c + 1) * 2)

        jobA = mgr.prebuild(
            "dummy",
            "0.3",
            input_files,
            output_files,
            calc2,
            minimum_acceptable_version="0.3",
        )
        ppg.FileGeneratingJob(
            "shu5",
            lambda: Path("shu5").write_text(Path(jobA.find_file("outA")).read_text()),
        ).depends_on(jobA)
        with pytest.raises(UpstreamChangedError):
            ppg.util.global_pipegraph.run()

        # this is also true if it was previously build on another machine
        new_pipeline.new_pipeline()
        mgr = PrebuildManager("prebuilt", "test_host2")

        def calc2(output_path):
            t = "\n".join([i.read_text() for i in input_files])
            c = int(count_file.read_text())
            (output_path / output_files[0]).write_text(t + str(c))
            count_file.write_text(str(c + 1) * 2)

        jobA = mgr.prebuild(
            "dummy",
            "0.3",
            input_files,
            output_files,
            calc2,
            minimum_acceptable_version="0.3",
        )
        ppg.FileGeneratingJob(
            "shu5",
            lambda: Path("shu5").write_text(Path(jobA.find_file("outA")).read_text()),
        ).depends_on(jobA)
        with pytest.raises(UpstreamChangedError):
            ppg.util.global_pipegraph.run()

        # but going back to the original -> ok
        new_pipeline.new_pipeline()
        mgr = PrebuildManager("prebuilt", "test_host2")

        def calc3(output_path):
            t = "\n".join([i.read_text() for i in input_files])
            c = int(count_file.read_text())
            (output_path / output_files[0]).write_text(t + str(c))
            count_file.write_text(str(c + 1))

        jobA = mgr.prebuild(
            "dummy",
            "0.3",
            input_files,
            output_files,
            calc3,
            minimum_acceptable_version="0.3",
        )
        ppg.FileGeneratingJob(
            "shu5",
            lambda: Path("shu5").write_text(Path(jobA.find_file("outA")).read_text()),
        ).depends_on(jobA)
        ppg.util.global_pipegraph.run()
        assert (
            Path("shu5").read_text() == "hello\nworld!2"
        )  # not rerun, neither the function nor the input files changed

    def test_chained(self, new_pipeline):
        Path("prebuilt").mkdir()
        mgr = PrebuildManager("prebuilt", "test_host")

        def calc_a(output_path):
            (output_path / "A").write_text("hello")

        jobA = mgr.prebuild("partA", "0.1", [], "A", calc_a)

        def calc_b(output_path):
            (output_path / "B").write_text(jobA.find_file("A").read_text() + " world")

        jobB = mgr.prebuild("partB", "0.1", [jobA.find_file("A")], "B", calc_b)
        jobB.depends_on(jobA)
        ppg.util.global_pipegraph.run()
        assert jobB.find_file("B").read_text() == "hello world"

    def test_chained2(self, new_pipeline):
        Path("prebuilt").mkdir()
        count_file = Path("count")
        count_file.write_text("0")
        mgr = PrebuildManager("prebuilt", "test_host")

        def calc_a(output_path):
            (output_path / "A").write_text("hello")
            c = int(count_file.read_text())
            count_file.write_text(str(c + 1))

        jobA = mgr.prebuild("partA", "0.1", [], "A", calc_a)
        ppg.util.global_pipegraph.run()
        assert jobA.find_file("A").read_text() == "hello"
        assert count_file.read_text() == "1"

        new_pipeline.new_pipeline()

        def calc_b(output_path):
            (output_path / "B").write_text(jobA.find_file("A").read_text() + " world")

        jobA = mgr.prebuild("partA", "0.1", [], "A", calc_a)

        jobB = mgr.prebuild("partB", "0.1", [jobA.find_file("A")], "B", calc_b)
        jobB.depends_on(jobA)
        ppg.util.global_pipegraph.run()
        assert jobB.find_file("B").read_text() == "hello world"
        assert count_file.read_text() == "1"

    def test_mixed_state_raises(self, new_pipeline):
        Path("prebuilt").mkdir()
        mgr = PrebuildManager("prebuilt", "test_host")

        def calc_a(output_path):
            (output_path / "A").write_text("hello")
            (output_path / "B").write_text("hello")

        jobA = mgr.prebuild("partA", "0.1", [], ["A", "B"], calc_a)
        jobA.find_file("A").write_text("something")
        with pytest.raises(ValueError):
            ppg.util.global_pipegraph.run()

    def test_prebuilt_job_raises_on_non_iterable(self, new_pipeline):
        from mbf_externals.prebuild import PrebuildJob

        with pytest.raises(TypeError):
            PrebuildJob(5, lambda: 5, "shu")
        with pytest.raises(TypeError):
            PrebuildJob([5], lambda: 5, "shu")
        with pytest.raises(ValueError):
            PrebuildJob([Path("shu").absolute()], lambda: 5, "shu")
