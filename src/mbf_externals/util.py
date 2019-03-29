import functools
import natsort


class lazy_property(object):
    """
    meant to be used for lazy evaluation of an object attribute.
    property should represent non-mutable data, as it replaces itself.
    """

    def __init__(self, fget):
        self.fget = fget

        # copy the getter function's docstring and other attributes
        functools.update_wrapper(self, fget)

    def __get__(self, obj, cls):
        # if obj is None: # this was in the original recepie, but I don't see
        # when it would be called?
        # return self

        value = self.fget(obj)
        setattr(obj, self.fget.__name__, value)
        return value


def lazy_method(func):
    """
    meant to be used for lazy evaluation of an object attribute.
    property should represent non-mutable data, as it replaces itself.
    """
    cache_name = "_cached_" + func.__name__

    def inner(self):
        if not hasattr(self, cache_name):
            setattr(self, cache_name, func(self))
        return getattr(self, cache_name)

    return inner


def sort_versions(versions):
    """Sort versions, from natsort manual:
        Sorts like this:
            ['1.1', '1.2', '1.2alpha', '1.2beta1', '1.2beta2', '1.2rc1', '1.2.1', '1.3']
    """
    return natsort.natsorted(
        versions,
        key=lambda x: x.replace(".", "~")
        if not isinstance(x, tuple)
        else x[0].replace(".", "~"),
    )


class Version:
    def __init__(self, version):
        self.version = version

    def __str__(self):
        return self.version

    def __repr__(self):
        return 'Version("%s")' % (self.version,)

    def __eq__(self, other_version):
        if isinstance(other_version, Version):
            other = other_version.version
        else:
            other = other_version
        return self.version == other

    def __lt__(self, other_version):
        if isinstance(other_version, Version):
            other = other_version.version
        else:
            other = other_version
        s = sort_versions([self.version, other])
        return s[0] == self.version and not self.version == other

    def __le__(self, other_version):
        return (self == other_version) or (self < other_version)

    def __gt__(self, other_version):
        if isinstance(other_version, Version):
            other = other_version.version
        else:
            other = other_version
        s = sort_versions([self.version, other])
        return s[1] == self.version and not self.version == other

    def __ge__(self, other_version):
        return (self == other_version) or (self > other_version)


class UpstreamChangedError(ValueError):
    pass


def download_file(url, file_object):
    """Download an url"""

    if isinstance(file_object, str):
        raise ValueError("dowload_file needs a file-object not a name")

    if url.startswith("ftp"):
        return download_ftp(url, file_object)
    else:
        return download_http(url, file_object)


def download_http(url, file_object):
    """Download a file from http"""
    import requests
    import shutil

    r = requests.get(url, stream=True)
    r.raw.decode_content = True
    shutil.copyfileobj(r.raw, file_object)


def download_ftp(url, file_object):
    """Download a file from ftp"""
    import ftplib
    import urllib

    schema, host, path, parameters, query, fragment = urllib.parse.urlparse(url)
    with ftplib.FTP(host) as ftp:
        try:
            ftp.login("anonymous", "")
            if path.endswith("/"):
                ftp.retrbinary("LIST " + path, file_object.write)
            else:
                ftp.retrbinary("RETR " + path, file_object.write)
        except ftplib.Error as e:
            raise ValueError("Error retrieving urls %s: %s" % (url, e))


def get_page(url):
    """Download a web page (http/ftp) into a string"""
    from io import BytesIO

    tf = BytesIO()
    download_file(url, tf)
    tf.seek(0, 0)
    return tf.read().decode("utf-8")


def download_file_and_gunzip(url, unzipped_filename):
    import shutil
    import gzip
    import tempfile

    tf = tempfile.NamedTemporaryFile(suffix=".gz")
    download_file(url, tf)
    tf.flush()

    with gzip.GzipFile(tf.name, "rb") as gz_in:
        with open(unzipped_filename, "wb") as op:
            shutil.copyfileobj(gz_in, op)


def write_md5_sum(filepath):
    """Create filepath.md5sum with the md5 hexdigest"""
    from pypipegraph.util import checksum_file

    md5sum = checksum_file(filepath)
    (filepath.with_name(filepath.name + ".md5sum")).write_text(md5sum)


def to_string(s, encoding="utf-8"):
    if isinstance(s, str):
        return s
    else:
        return s.decode(encoding)


def to_bytes(x, encoding="utf-8"):
    """ In python3: str -> bytes. Bytes stay bytes"""
    if isinstance(x, bytes):
        return x
    else:
        return x.encode(encoding)


def chmod(filename, mode):
    """Chmod if possible - otherwise try to steal the file and chmod then"""
    import os
    import shutil

    try:
        os.chmod(filename, mode)
    except OSError as e:  # pragma: no cover
        if (
            str(e).find("Operation not permitted") == -1
            and str(e).find("Permission denied") == -1
        ):
            raise
        else:  # steal ownership and set the permissions...
            t = filename + ".temp"
            shutil.copyfile(filename, t)
            try:
                os.chmod(t, mode)
            except OSError:
                pass
            os.unlink(filename)
            shutil.move(t, filename)
