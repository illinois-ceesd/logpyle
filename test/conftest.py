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
