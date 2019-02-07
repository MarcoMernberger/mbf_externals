import pytest
import pypipegraph as ppg
from pathlib import Path
from mbf_externals import PrebuildManager
from mbf_externals.util import UpstreamChangedError


class TestPrebuilt:
    def test_simple(self, new_pipegraph):
        new_pipegraph.quiet = False
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
        assert Path("prebuilt/test_host/dummy/0.1/outA.md5sum").exists()
        assert Path("shu").read_text() == "hello\nworld0"

        # no rerunning.
        new_pipegraph = new_pipegraph.new_pipegraph()
        jobA = mgr.prebuild("dummy", "0.1", input_files, output_files, calc)
        ppg.FileGeneratingJob(
            "shu",
            lambda: Path("shu").write_text(Path(jobA.find_file("outA")).read_text()),
        ).depends_on(jobA)
        ppg.util.global_pipegraph.run()
        assert Path("prebuilt/test_host/dummy/0.1/outA").read_text() == "hello\nworld0"
        assert Path("shu").read_text() == "hello\nworld0"

        # no rerunning, getting from second path...
        new_pipegraph = new_pipegraph.new_pipegraph()
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
        new_pipegraph.new_pipegraph()
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
        new_pipegraph.new_pipegraph()
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
        new_pipegraph.new_pipegraph()
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
        new_pipegraph.new_pipegraph()
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
        new_pipegraph.new_pipegraph()
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
        new_pipegraph.new_pipegraph()
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
        new_pipegraph.new_pipegraph()
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
        new_pipegraph.new_pipegraph()
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

    def test_chained(self, new_pipegraph):
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

    def test_chained2(self, new_pipegraph):
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

        new_pipegraph.new_pipegraph()

        def calc_b(output_path):
            (output_path / "B").write_text(jobA.find_file("A").read_text() + " world")

        jobA = mgr.prebuild("partA", "0.1", [], "A", calc_a)

        jobB = mgr.prebuild("partB", "0.1", [jobA.find_file("A")], "B", calc_b)
        jobB.depends_on(jobA)
        ppg.util.global_pipegraph.run()
        assert jobB.find_file("B").read_text() == "hello world"
        assert count_file.read_text() == "1"

    def test_mixed_state_raises(self, new_pipegraph):
        Path("prebuilt").mkdir()
        mgr = PrebuildManager("prebuilt", "test_host")

        def calc_a(output_path):
            (output_path / "A").write_text("hello")
            (output_path / "B").write_text("hello")

        jobA = mgr.prebuild("partA", "0.1", [], ["A", "B"], calc_a)
        jobA.find_file("A").write_text("something")
        with pytest.raises(ValueError):
            ppg.util.global_pipegraph.run()

    def test_prebuilt_job_raises_on_non_iterable(self, new_pipegraph):
        from mbf_externals.prebuild import PrebuildJob

        with pytest.raises(TypeError):
            PrebuildJob(5, lambda: 5, "shu")
        with pytest.raises(TypeError):
            PrebuildJob([5], lambda: 5, "shu")
        with pytest.raises(ValueError):
            PrebuildJob([Path("shu").absolute()], lambda: 5, "shu")

    def test_minimal_and_maximal_versions(self, new_pipegraph):
        Path("prebuilt").mkdir()
        count_file = Path("count")
        count_file.write_text("0")
        mgr = PrebuildManager("prebuilt", "test_host")

        def calc_04(output_path):
            (output_path / "A").write_text("0.4")
            c = int(count_file.read_text())
            count_file.write_text(str(c + 1))

        def calc_05(output_path):
            (output_path / "A").write_text("0.5")
            c = int(count_file.read_text())
            count_file.write_text(str(c + 1))

        def calc_06(output_path):
            (output_path / "A").write_text("0.6")
            c = int(count_file.read_text())
            count_file.write_text(str(c + 1))

        def calc_07(output_path):
            (output_path / "A").write_text("0.7")
            c = int(count_file.read_text())
            count_file.write_text(str(c + 1))

        jobA = mgr.prebuild("partA", "0.5", [], "A", calc_05)
        ppg.FileGeneratingJob(
            "checkme",
            lambda: Path("checkme").write_text(jobA.find_file("A").read_text()),
        ).depends_on(jobA)
        ppg.util.global_pipegraph.quiet = False
        ppg.util.global_pipegraph.run()
        assert jobA.find_file("A").read_text() == "0.5"
        assert Path("checkme").read_text() == "0.5"
        assert count_file.read_text() == "1"

        # no rerun here
        new_pipegraph.new_pipegraph()
        jobA = mgr.prebuild(
            "partA",
            "0.5",
            [],
            "A",
            calc_05,
            minimum_acceptable_version="0.3",
            maximum_acceptable_version="0.6",
        )
        ppg.FileGeneratingJob(
            "checkme",
            lambda: Path("checkme").write_text(jobA.find_file("A").read_text()),
        ).depends_on(jobA).depends_on_params(jobA.version)

        ppg.util.global_pipegraph.run()
        assert jobA.find_file("A").read_text() == "0.5"
        assert Path("checkme").read_text() == "0.5"
        assert count_file.read_text() == "1"

        # no rerun on I want this exact version
        new_pipegraph.new_pipegraph()
        jobA = mgr.prebuild(
            "partA",
            "0.5",
            [],
            "A",
            calc_05,
            minimum_acceptable_version="0.5",
            maximum_acceptable_version="0.5",
        )
        ppg.FileGeneratingJob(
            "checkme",
            lambda: Path("checkme").write_text(jobA.find_file("A").read_text()),
        ).depends_on(jobA).depends_on_params(jobA.version)

        ppg.util.global_pipegraph.run()
        assert jobA.find_file("A").read_text() == "0.5"
        assert Path("checkme").read_text() == "0.5"
        assert count_file.read_text() == "1"

        # but we don't have this one
        new_pipegraph.new_pipegraph()
        ppg.util.global_pipegraph.quiet = False
        jobA = mgr.prebuild(
            "partA",
            "0.6",
            [],
            "A",
            calc_06,
            minimum_acceptable_version="0.6",
            maximum_acceptable_version=None,
        )
        ppg.FileGeneratingJob(
            "checkme",
            lambda: Path("checkme").write_text(jobA.find_file("A").read_text()),
        ).depends_on(jobA).depends_on_params(jobA.version)

        ppg.util.global_pipegraph.run()
        assert jobA.find_file("A").read_text() == "0.6"
        assert Path("checkme").read_text() == "0.6"
        assert count_file.read_text() == "2"

        # again no rerun
        new_pipegraph.new_pipegraph()
        jobA = mgr.prebuild(
            "partA",
            "0.6",
            [],
            "A",
            calc_06,
            minimum_acceptable_version="0.5",
            maximum_acceptable_version=None,
        )
        ppg.FileGeneratingJob(
            "checkme",
            lambda: Path("checkme").write_text(jobA.find_file("A").read_text()),
        ).depends_on(jobA).depends_on_params(jobA.version)

        ppg.util.global_pipegraph.run()
        assert jobA.find_file("A").read_text() == "0.6"
        assert Path("checkme").read_text() == "0.6"
        assert jobA.version == "0.6"  # since 0.6 was build
        assert count_file.read_text() == "2"

        # get an older one
        new_pipegraph.new_pipegraph()
        jobA = mgr.prebuild(
            "partA",
            "0.4",
            [],
            "A",
            calc_04,
            minimum_acceptable_version=None,
            maximum_acceptable_version="0.4",
        )
        ppg.FileGeneratingJob(
            "checkme",
            lambda: Path("checkme").write_text(jobA.find_file("A").read_text()),
        ).depends_on(jobA).depends_on_params(jobA.version)

        ppg.util.global_pipegraph.run()
        assert jobA.find_file("A").read_text() == "0.4"
        assert Path("checkme").read_text() == "0.4"
        assert count_file.read_text() == "3"

        # you want 0.4-.. you get' the 0.6
        new_pipegraph.new_pipegraph()
        jobA = mgr.prebuild(
            "partA",
            "0.7",
            [],
            "A",
            calc_06,  # no change here...
            minimum_acceptable_version="0.4",
            maximum_acceptable_version=None,
        )
        ppg.FileGeneratingJob(
            "checkme",
            lambda: Path("checkme").write_text(jobA.find_file("A").read_text()),
        ).depends_on(jobA).depends_on_params(jobA.version)

        ppg.util.global_pipegraph.run()
        assert jobA.find_file("A").read_text() == "0.6"
        assert Path("checkme").read_text() == "0.6"
        assert jobA.version == "0.6"
        assert count_file.read_text() == "3"  # no rerun of the build

        # you want 0.4-.. but you changed the build func -> 0.7
        new_pipegraph.new_pipegraph()
        jobA = mgr.prebuild(
            "partA",
            "0.7",
            [],
            "A",
            calc_07,  # no change here...
            minimum_acceptable_version="0.4",
            maximum_acceptable_version=None,
        )
        ppg.FileGeneratingJob(
            "checkme",
            lambda: Path("checkme").write_text(jobA.find_file("A").read_text()),
        ).depends_on(jobA).depends_on_params(jobA.version)

        ppg.util.global_pipegraph.run()
        # and no rebuild
        assert jobA.find_file("A").read_text() == "0.7"
        assert Path("checkme").read_text() == "0.7"
        assert jobA.version == "0.7"
        assert count_file.read_text() == "4"

        # you want 0.5 min, with an 05 build func, you get 0.5 - and a rerun
        new_pipegraph.new_pipegraph()
        jobA = mgr.prebuild(
            "partA",
            "0.5",
            [],
            "A",
            calc_05,
            minimum_acceptable_version="0.5",
            maximum_acceptable_version=None,
        )
        ppg.FileGeneratingJob(
            "checkme",
            lambda: Path("checkme").write_text(jobA.find_file("A").read_text()),
        ).depends_on(jobA).depends_on_params(jobA.version)

        ppg.util.global_pipegraph.run()
        assert jobA.find_file("A").read_text() == "0.5"
        assert Path("checkme").read_text() == "0.5"
        assert jobA.version == "0.5"
        assert count_file.read_text() == "4"

        # and at last, we want 0.5
        new_pipegraph.new_pipegraph()
        jobA = mgr.prebuild(
            "partA",
            "0.5",
            [],
            "A",
            calc_05,
            minimum_acceptable_version="0.5",
            maximum_acceptable_version="0.5",
        )
        ppg.FileGeneratingJob(
            "checkme",
            lambda: Path("checkme").write_text(jobA.find_file("A").read_text()),
        ).depends_on(jobA).depends_on_params(jobA.version)

        ppg.util.global_pipegraph.run()
        assert jobA.find_file("A").read_text() == "0.5"
        assert Path("checkme").read_text() == "0.5"
        assert jobA.version == "0.5"
        assert count_file.read_text() == "4"
