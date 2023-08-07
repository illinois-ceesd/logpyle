import os

import pytest

from logpyle import LogManager


def cleanup_mpi_files():
    def is_unique_filename(str: str):
        return str.startswith("THIS_LOG_SHOULD_BE_DELETED-")

    files = [f for f in os.listdir() if is_unique_filename(f)]
    for f in files:
        os.remove(f)


@pytest.fixture
def basic_logmgr():
    import os

    # setup
    filename = "THIS_LOG_SHOULD_BE_DELETED.sqlite"
    logmgr = LogManager(filename, "wo")
    # give obj to test
    yield logmgr
    # cleanup object
    logmgr.close()
    os.remove(filename)


@pytest.fixture
def basic_distributed():
    yield
    cleanup_mpi_files()
    print("cleaned up mpi files")


def assert_cov(old_filename: str, new_filename: str):
    import json

    new_f = open(new_filename)
    old_f = open(old_filename)

    new = json.load(new_f)
    old = json.load(old_f)

    new_cov = new["totals"]["percent_covered"]
    old_cov = old["totals"]["percent_covered"]

    new_f.close()
    old_f.close()
    if new_cov < old_cov:
        raise Exception(f"New coverage is less than old coverage.\
                {old_cov}->{new_cov}")
    else:
        print("Test coverage is acceptable")
