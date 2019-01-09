from pathlib import Path
import time
import os
import subprocess
from abc import ABC, abstractmethod
import pypipegraph as ppg
from .util import lazy_property, sort_versions

_global_store = None


def change_global_store(new_store):
    global _global_store
    _global_store = new_store


class ExternalAlgorithm(ABC):
    """Together with an ExternalAlgorithmStore (or the global one),
    ExternalAlgorithm encapsulates a callable algorithm such as a high throughput aligner.
    """

    def __init__(self, version="_last_used", store=None):
        """
        Parameters
        ----------
            version: str
            either one of the available versions from the store,
            _latest (always the latest!) or
            _last_used  - the last used one, or the newes if this is the first time
                (stored '.mbf_external_versions' )

        """

        if store is None:
            store = _global_store
        self.store = store

        if version == "_last_used":
            actual_version = self._last_used_version
            if actual_version is None:
                actual_version = "_latest"
        else:
            actual_version = version
        if actual_version == "_latest":
            try:
                self.version = store.get_available_versions(self.name)[-1]
            except IndexError:
                self.fetch_latest_version()
                self.version = store.get_available_versions(self.name)[-1]
        elif actual_version == "_fetching":  # pragma: no cover
            self.version = "_fetching"
        else:
            if actual_version in store.get_available_versions(self.name):
                self.version = actual_version
            else:
                raise ValueError(
                    f"Version '{actual_version}' not found for algorithm {self.name}"
                )
        self._store_used_version()
        self.path = store.get_unpacked_path(self.name, self.version)

    @lazy_property
    def _last_used_version(self):
        try:
            lines = Path(".mbf_external_versions").read_text().strip().split("\n")
            for l in lines:
                if l.strip():
                    name, version = l.split("==")
                    if name == self.name:
                        return version
        except OSError:
            pass
        return None

    def _store_used_version(self):
        last_used = self._last_used_version
        if (
            last_used is None
            or sort_versions([last_used, self.version])[0] == last_used
        ):
            try:
                p = Path(".mbf_external_versions")
                lines = p.read_text().strip().split("\n")
                lines = [x for x in lines if not x.startswith(self.name + "==")]
            except OSError:
                lines = []
            lines.append(f"{self.name}=={self.version}")
            p.write_text("\n".join(lines) + "\n")

    @property
    @abstractmethod
    def name(self):
        pass  # pragma: no cover

    @abstractmethod
    def build_cmd(self, output_directory, ncores, arguments):
        pass  # pragma: no cover

    @property
    def multi_core(self):
        return False

    def run(self, output_directory, arguments=None, cwd=None, call_afterwards=None):
        """Return a job that runs the algorithm and puts the
        results in output_directory.
        Note that assigning different ouput_directories to different
        versions is your problem.
        """
        output_directory = Path(output_directory)
        output_directory.mkdir(parents=True, exist_ok=True)
        sentinel = output_directory / "sentinel.txt"

        job = ppg.FileGeneratingJob(
            sentinel,
            self.get_run_func(
                output_directory, arguments, cwd=cwd, call_afterwards=call_afterwards
            ),
        ).depends_on(
            ppg.FileChecksumInvariant(
                self.store.get_zip_file_path(self.name, self.version)
            ),
            ppg.FunctionInvariant(str(sentinel) + "_call_afterwards", call_afterwards),
        )
        if self.multi_core:
            job.cores_needed = -1
        return job

    def get_run_func(self, output_directory, arguments, cwd=None, call_afterwards=None):
        def do_run():
            self.store.unpack_version(self.name, self.version)
            sentinel = output_directory / "sentinel.txt"
            stdout = output_directory / "stdout.txt"
            stderr = output_directory / "stderr.txt"
            cmd_out = output_directory / "cmd.txt"

            op_stdout = open(stdout, "wb")
            op_stderr = open(stderr, "wb")
            cmd = [
                str(x)
                for x in self.build_cmd(
                    output_directory,
                    ppg.util.global_pipegraph.rc.cores_available
                    if self.multi_core
                    else 1,
                    arguments,
                )
            ]
            cmd_out.write_text(repr(cmd))
            start_time = time.time()
            p = subprocess.Popen(cmd, stdout=op_stdout, stderr=op_stderr, cwd=cwd)
            p.communicate()
            op_stdout.close()
            op_stderr.close()
            ok = self.check_success(
                p.returncode, stdout.read_bytes(), stderr.read_bytes()
            )
            if ok is True:
                runtime = time.time() - start_time
                sentinel.write_text(
                    f"run time: {runtime:.2f} seconds\nreturn code: {p.returncode}"
                )
                if call_afterwards is not None:
                    call_afterwards()
            else:
                raise ValueError(
                    f"{self.name} run failed. Error was: {ok}. Cmd was: {cmd}"
                )

        return do_run

    def check_success(self, return_code, stdout, stderr):
        if return_code == 0:
            return True
        else:
            return f"Return code != 0: {return_code}"

    def fetch_latest_version(self):  # pragma: no cover
        pass


class ExternalAlgorithmStore:
    def __init__(self, zip_path, unpack_path):
        self.zip_path = Path(zip_path)
        self.unpack_path = Path(unpack_path)
        self._version_cache = {}

    def get_available_versions(self, algorithm_name):
        if (
            not algorithm_name in self._version_cache
            or not self._version_cache[algorithm_name]
        ):
            matching = self.zip_path.glob(f"{algorithm_name}__*.tar.gz")
            versions = [x.stem[x.stem.find("__") + 2 : -4] for x in matching]
            self._version_cache[algorithm_name] = sort_versions(versions)
        return self._version_cache[algorithm_name]

    def unpack_version(self, algorithm_name, version):
        if not version in self.get_available_versions(algorithm_name):
            raise ValueError("No such version")
        target_path = self.get_unpacked_path(algorithm_name, version)
        sentinel = target_path / "unpack_done.txt"
        if sentinel.exists():
            return
        target_path.mkdir(parents=True, exist_ok=True)
        gzip_path = self.get_zip_file_path(algorithm_name, version)
        subprocess.check_call(["tar", "-xf", gzip_path], cwd=target_path)
        sentinel.write_text("Done")

    def get_unpacked_path(self, algorithm_name, version):
        return self.unpack_path / algorithm_name / version

    def get_zip_file_path(self, algorithm_name, version):
        return self.zip_path / (algorithm_name + "__" + version + ".tar.gz")


def virtual_env_store():
    zipped = Path(os.environ["VIRTUAL_ENV"]) / "mbf_store" / "zip"
    unpacked = Path(os.environ["VIRTUAL_ENV"]) / "mbf_store" / "unpack"
    zipped.mkdir(exist_ok=True, parents=True)
    unpacked.mkdir(exist_ok=True, parents=True)
    change_global_store(ExternalAlgorithmStore(zipped, unpacked))


if "VIRTUAL_ENV" in os.environ:
    virtual_env_store()
