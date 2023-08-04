import pytest

from logpyle import LogManager


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
        raise Exception(f"New coverage is less than old coverage. {old_cov}->{new_cov}")
    else:
        print("Test coverage is acceptable")
