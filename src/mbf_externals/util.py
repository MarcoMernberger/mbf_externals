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


def compare_versions(a, b):
    """Return true if b >= a"""
    if a == b:
        return True
    s = sort_versions([a, b])
    return s[0] == b


class UpstreamChangedError(ValueError):
    pass
