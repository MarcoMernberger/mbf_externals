"""
Many algorithms need prebuild data structures (indices and so on)
which both are too time consuming to build, to big to copy to each
experiment and need to be versioned,
but they can often be shared among versions."""

import socket
from .util import Version, sort_versions, UpstreamChangedError, write_md5_sum
import hashlib
import pypipegraph as ppg
from pathlib import Path
import time
import stat
import os


class PrebuildFunctionInvariantFileStoredExploding(ppg.FunctionInvariant):
    def __init__(self, storage_filename, func):
        super().__init__(storage_filename, func)

    @classmethod
    def hash_function(cls, function):
        if hasattr(function, "im_func") and "cyfunction" in repr(function.im_func):
            invariant = cls.get_cython_source(function)  # pragma: no cover
        else:
            invariant = cls.dis_code(function.__code__, function)
        invariant_hash = hashlib.md5(invariant.encode("utf-8")).hexdigest()
        return invariant_hash

    def _get_invariant(self, old, all_invariant_stati):
        invariant_hash = self.hash_function(self.function)
        stf = Path(self.job_id)
        if stf.exists():
            old_hash = stf.read_text()
            if old_hash != invariant_hash:
                raise UpstreamChangedError(
                    "Calculating function changed, bump version or rollback"
                )
        else:
            stf.write_text(invariant_hash)
        return old  # signal no invariant change


class PrebuildFileInvariantsExploding(ppg.MultiFileInvariant):
    def __new__(cls, job_id, filenames):
        job_id = "PFIE_" + str(job_id)
        return ppg.Job.__new__(cls, job_id)

    def __init__(self, job_id, filenames):
        job_id = "PFIE_" + str(job_id)
        self.filenames = filenames
        ppg.Job.__init__(self, job_id)

    def calc_checksums(self, old):
        """return a list of tuples
        (filename, filetime, filesize, checksum)"""
        result = []
        if old:
            old_d = {x[0]: x[1:] for x in old}
        else:
            old_d = {}
        for fn in self.filenames:
            if not os.path.exists(fn):
                result.append((fn, None, None, None))
            else:
                st = os.stat(fn)
                filetime = st[stat.ST_MTIME]
                filesize = st[stat.ST_SIZE]
                if (
                    fn in old_d
                    and (old_d[fn][0] == filetime)
                    and (old_d[fn][1] == filesize)
                ):  # we can reuse the checksum
                    result.append((fn, filetime, filesize, old_d[fn][2]))
                else:
                    result.append((fn, filetime, filesize, ppg.util.checksum_file(fn)))
        return result

    def _get_invariant(self, old, all_invariant_stati):
        if not old:
            old = self.find_matching_renamed(all_invariant_stati)
        checksums = self.calc_checksums(old)
        if old is False:
            raise ppg.ppg_exceptions.NothingChanged(checksums)
        # elif old is None: # not sure when this would ever happen
        # return checksums
        else:
            old_d = {x[0]: x[1:] for x in old}
            checksums_d = {x[0]: x[1:] for x in checksums}
            for fn in self.filenames:
                if old_d[fn][2] != checksums_d[fn][2] and old_d[fn][2] is not None:
                    raise UpstreamChangedError(
                        """Upstream file changed for job, bump version or rollback.
Job: %s
File: %s"""
                        % (self, fn)
                    )
                    # return checksums
            raise ppg.ppg_exceptions.NothingChanged(checksums)


class PrebuildJob(ppg.MultiFileGeneratingJob):
    def __new__(cls, filenames, calc_function, output_path):
        if not hasattr(filenames, "__iter__"):
            raise TypeError("filenames was not iterable")
        for x in filenames:
            if not (isinstance(x, str) or isinstance(x, Path)):
                raise TypeError("filenames must be a list of strings or pathlib.Path")
        for of in filenames:
            if of.is_absolute():
                raise ValueError("output_files must be relative")
        filenames = cls._normalize_output_files(filenames, output_path)
        job_id = ":".join(sorted(str(x) for x in filenames))
        return ppg.Job.__new__(cls, job_id)

    @classmethod
    def _normalize_output_files(cls, output_files, output_path):
        output_files = [output_path / of for of in output_files]
        output_files.append(output_path / "mbf.done")
        return output_files

    def __init__(self, output_files, calc_function, output_path):
        output_files = self._normalize_output_files(output_files, output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        self.real_callback = calc_function

        def calc():
            self.real_callback(output_path)
            output_files[-1].write_text(str(time.time()))
            for fn in output_files[:-1]:
                if os.path.exists(fn):
                    write_md5_sum(fn)

        super().__init__(output_files, calc, rename_broken=True, empty_ok=True)
        self.output_path = output_path

    def inject_auto_invariants(self):
        self.depends_on(
            PrebuildFunctionInvariantFileStoredExploding(
                self.output_path / "mbf_func.md5sum", self.real_callback
            )
        )

    def invalidated(self, reason):
        exists = [Path(of).exists() for of in self.filenames]
        if all(exists) or not any(exists):
            pass
        else:
            raise ValueError(
                "Some output files existed, some don't - undefined state, manual cleanup needed"
            )
        self.was_invalidated = True

    def name_file(self, output_filename):
        """Adjust path of output_filename by job path"""
        return self.output_path / output_filename

    def find_file(self, output_filename):
        """Search for a file named output_filename in the job's known created files"""
        of = self.name_file(output_filename)
        for fn in self.filenames:
            if of.resolve() == Path(fn).resolve():
                return of
        else:
            raise KeyError("file not found: %s" % output_filename)


class PrebuildManager:
    def __init__(self, prebuilt_path, hostname=None):
        self.prebuilt_path = Path(prebuilt_path)
        self.hostname = hostname if hostname else socket.gethostname()
        (self.prebuilt_path / self.hostname).mkdir(exist_ok=True)

    def _find_versions(self, name):
        result = {}
        dirs_to_consider = [
            p
            for p in self.prebuilt_path.glob("*")
            if (p / name).exists() and p.name != self.hostname
        ]
        # prefer versions from this host - must be last!
        dirs_to_consider.append(self.prebuilt_path / self.hostname)
        for p in dirs_to_consider:
            for v in (p / name).glob("*"):
                if (v / "mbf.done").exists():
                    result[v.name] = v
        return result

    def prebuild(
        self,
        name,
        version,
        input_files,
        output_files,
        calculating_function,
        minimum_acceptable_version=None,
        maximum_acceptable_version=None,
    ):
        """Create a job that will prebuilt the files if necessary"""
        if minimum_acceptable_version is None:
            minimum_acceptable_version = version

        available_versions = self._find_versions(name)
        if version in available_versions:
            output_path = available_versions[version]
        else:
            # these are within minimum..maximum_acceptable_version
            acceptable_versions = sort_versions(
                [
                    (v, p)
                    for v, p in available_versions.items()
                    if (
                        (Version(v) >= minimum_acceptable_version)
                        and (
                            maximum_acceptable_version is None
                            or (Version(v) < maximum_acceptable_version)
                        )
                    )
                ]
            )
            ok_versions = []
            calculating_function_md5_sum = PrebuildFunctionInvariantFileStoredExploding.hash_function(
                calculating_function
            )
            for v, p in acceptable_versions:
                func_md5sum_path = p / "mbf_func.md5sum"
                func_md5sum = func_md5sum_path.read_text()
                if func_md5sum == calculating_function_md5_sum:
                    ok_versions.append((v, p))

            if ok_versions:
                version, output_path = ok_versions[-1]
            else:  # no version that is within the acceptable range and had the same build function
                output_path = self.prebuilt_path / self.hostname / name / version

        if isinstance(output_files, (str, Path)):
            output_files = [output_files]
        output_files = [Path(of) for of in output_files]
        job = PrebuildJob(output_files, calculating_function, output_path)
        job.depends_on(PrebuildFileInvariantsExploding(output_path, input_files))
        job.version = version
        return job


_global_manager = None


def change_global_manager(new_manager):
    global _global_manager
    _global_manager = new_manager


def get_global_manager():
    return _global_manager
