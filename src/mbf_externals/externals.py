from pathlib import Path
import time
import os
import subprocess
from abc import ABC, abstractmethod
import natsort
import pypipegraph as ppg

_global_store = None


def change_global_store(new_store):
    global _global_store
    _global_store = new_store


class ExternalAlgorithm(ABC):
    def __init__(self, version="latest", store=None):
        if store is None:
            store = _global_store
        self.store = store
        if version == "latest":
            try:
                self.version = store.get_available_versions(self.name)[-1]
            except IndexError:
                self.fetch_latest_version()
                self.version = store.get_available_versions(self.name)[-1]
        elif version == "fetching":  # pragma: no cover
            self.version = "fetching"
        else:
            if version in store.get_available_versions(self.name):
                self.version = version
            else:
                raise ValueError(
                    f"Version {version} not found for algorithm {self.name}"
                )
        self.path = store.get_unpacked_path(self.name, self.version)

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

    def run(self, output_directory, arguments=None):
        output_directory = Path(output_directory)
        true_output_directory = output_directory / self.version
        true_output_directory.mkdir(parents=True, exist_ok=True)
        sentinel = true_output_directory / "sentinel.txt"
        stdout = true_output_directory / "stdout.txt"
        stderr = true_output_directory / "stderr.txt"
        cmd_out = true_output_directory / "cmd.txt"

        def do_run():
            self.store.unpack_version(self.name, self.version)
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
            p = subprocess.Popen(cmd, stdout=op_stdout, stderr=op_stderr)
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
            else:
                raise ValueError(f"{self.name} run failed. Error was: {ok}")

        job = ppg.FileGeneratingJob(sentinel, do_run).depends_on(
            ppg.FileChecksumInvariant(
                self.store.get_zip_file_path(self.name, self.version)
            )
        )
        if self.multi_core:
            job.cores_needed = -1
        return job

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
            self._version_cache[algorithm_name] = natsort.natsorted(versions)
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
