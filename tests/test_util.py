from mbf_externals.util import to_string, to_bytes, chmod
import os


def test_to_string():
    a = "für".encode("utf-8")
    b = "für"
    assert to_string(b) is b
    assert to_string(a) == b


def test_to_bytes():
    a = "für".encode("utf-8")
    b = "für"
    assert to_bytes(b) == a
    assert to_bytes(a) is a


def test_chmod():
    import tempfile
    tf = tempfile.NamedTemporaryFile()
    assert not os.access(tf.name, os.X_OK)
    chmod(tf.name, 0o777)
    assert os.access(tf.name, os.X_OK)

def test_lazy_method():
    aoeu
