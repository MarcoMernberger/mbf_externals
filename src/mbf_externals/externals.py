from pathlib import Path
import time
import subprocess
import os
import stat
from abc import ABC, abstractmethod
import pypipegraph as ppg
from .util import lazy_property, sort_versions

_global_store = None


def change_global_store(new_store):
    global _global_store
    _global_store = new_store


def get_global_store():
    return _global_store


class DownloadDiscrepancyException(ValueError):
    pass


def reproducible_tar(target_tar, folder, cwd):
    """Create tars that look the same every time."""
    # see http://h2.jaguarpaw.co.uk/posts/reproducible-tar/

    target_tar = str(target_tar)
    folder = str(folder)

    cmd = [
        "tar",
        "--format=posix",
        "--pax-option=exthdr.name=%d/PaxHeaders/%f,delete=atime,delete=ctime,delete=mtime",
        "--mtime=1970-01-01 00:00:00Z",
        "--sort=name",
        "--numeric-owner",
        "--owner=0",
        "--group=0",
        "--mode=go+rwX,u+rwX",
        "-cvf",
        target_tar,
        folder,
    ]
    subprocess.check_call(cmd, cwd=cwd)


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
            self.version = self.get_latest_version()
            self._fetch_and_check(self.version)
        elif actual_version == "_fetching":  # pragma: no cover
            self.version = "_fetching"
        else:
            if actual_version in store.get_available_versions(self.name):
                self.version = actual_version
            else:
                self._fetch_and_check(actual_version)
                self.version = actual_version
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

    def run(
        self,
        output_directory,
        arguments=None,
        cwd=None,
        call_afterwards=None,
        additional_files_created=None,
    ):
        """Return a job that runs the algorithm and puts the
        results in output_directory.
        Note that assigning different ouput_directories to different
        versions is your problem.
        """
        output_directory = Path(output_directory)
        output_directory.mkdir(parents=True, exist_ok=True)
        sentinel = output_directory / "sentinel.txt"
        filenames = [sentinel]
        if additional_files_created:
            if isinstance(additional_files_created, (str, Path)):
                additional_files_created = [additional_files_created]
            filenames.extend(additional_files_created)

        job = ppg.MultiFileGeneratingJob(
            filenames,
            self.get_run_func(
                output_directory, arguments, cwd=cwd, call_afterwards=call_afterwards
            ),
        ).depends_on(
            ppg.FileChecksumInvariant(
                self.store.get_zip_file_path(self.name, self.version)
            ),
            ppg.FunctionInvariant(str(sentinel) + "_call_afterwards", call_afterwards),
        )
        job.ignore_code_changes()
        job.depends_on(
            ppg.FunctionInvariant(
                job.job_id + "_build_cmd_func", self.__class__.build_cmd
            )
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

    def _fetch_and_check(self, version):
        if self.store.no_downloads:
            print("WARNING: Downloads disabled for this store")
            return
        target_filename = self.store.get_zip_file_path(self.name, version).absolute()
        if target_filename.exists():
            return
        self.fetch_version(version, target_filename)
        try:
            checksum = ppg.util.checksum_file(target_filename)
        except OSError:  # pragma: no cover
            raise ValueError("Algorithm did not download correctly")
        md5_file = target_filename.with_name(target_filename.name + ".md5sum")
        st = os.stat(target_filename)
        with open(md5_file, "wb") as op:
            op.write(checksum.encode("utf-8"))
        os.utime(md5_file, (st[stat.ST_MTIME], st[stat.ST_MTIME]))
        self._check_hash_against_others(target_filename, checksum)

    def _check_hash_against_others(self, target_filename, checksum):
        """See if another machine has downloaded the file and synced it's mbf_store.
        If so, look at it's hash. If it differs, throw an Exception"""
        search_path = self.store.zip_path.absolute().parent.parent.parent
        print(search_path)
        search_key = "**/" + self.store.zip_path.name + "/" + target_filename.name
        by_hash = {checksum: [target_filename]}
        for found in search_path.glob(search_key):
            print("found", found)
            if found != target_filename:
                cs = ppg.util.checksum_file(found)
                if not cs in by_hash:
                    by_hash[cs] = []
                by_hash[cs].append(found)
        if len(by_hash) > 1:
            import pprint

            pprint.pprint(by_hash)
            raise DownloadDiscrepancyException(
                f"Found multiple different {target_filename.name} with different md5sum. Investitage and fix (possibly using reproducible_tar), please."
            )

    def fetch_version(self, version, target_filename):  # pragma: no cover
        # overwrite this in the downstream algorithms
        raise NotImplementedError()
        pass


class ExternalAlgorithmStore:
    def __init__(self, zip_path, unpack_path, no_downloads=False):
        self.zip_path = Path(zip_path)
        self.unpack_path = Path(unpack_path)
        self.no_downloads = no_downloads
        self._version_cache = {}

    def get_available_versions(self, algorithm_name):
        if (
            not algorithm_name in self._version_cache
            or not self._version_cache[algorithm_name]
        ):
            glob = f"{algorithm_name}__*.tar.gz"
            matching = list(self.zip_path.glob(glob))
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
