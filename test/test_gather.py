import logging
import os
from warnings import warn

from logpyle import (LogManager, LogQuantity, add_general_quantities,
                     add_run_info)
from logpyle.runalyzer import make_wrapped_db


class Fifteen(LogQuantity):
    def __call__(self) -> int:
        return 15


def create_log(filename: str):
    logmgr = LogManager(filename, "wo")

    # Generic run metadata, such as command line, host, and time
    add_run_info(logmgr)

    # Time step duration, wall time, ...
    add_general_quantities(logmgr)

    logmgr.add_quantity(Fifteen("fifteen"))

    logger = logging.getLogger(__name__)

    for i in range(20):
        print(i)
        logmgr.tick_before()

        if i == 5:
            warn("warning from fifth timestep")

        if i == 10:
            logger.warning("test on tenth timestep")

        # do something ...
        logmgr.tick_after()

    logmgr.close()


def test_auto_gather_single():
    # run example
    create_log("log.sqlite")
    assert os.path.exists("log.sqlite"), "The logging file was not generated."

    # check schema

    # ensure quantity table exists
    db = make_wrapped_db(["log.sqlite"], mangle=True, interactive=False)
    cur = db.q("select * from quantities")
    print("Quantity data:")
    result = list(cur)
    print(result)
    assert len(result) == 8

    # ensure warnings table exists
    db = make_wrapped_db(["log.sqlite"], mangle=True, interactive=False)
    cur = db.q("select * from warnings")
    print("Warnings data:")
    result = list(cur)
    print(result)
    assert len(result) == 1

    # ensure logging in runs table exists
    db = make_wrapped_db(["log.sqlite"], mangle=True, interactive=False)
    cur = db.q("select * from logging")
    print("Logging data:")
    result = list(cur)
    print(result)
    assert len(result) == 1

    # check constant
    db = make_wrapped_db(["log.sqlite"], mangle=True, interactive=False)
    cur = db.q("select * from runs")
    print("Constant data:")
    result = [row[0] for row in cur.description]
    print(result)
    assert len(result) == 11

    db = make_wrapped_db(["log.sqlite"], mangle=True, interactive=False)
    cur = db.q("select $fifteen")
    print("Fifteen data:")
    result = [row[0] for row in cur]
    print(result)
    assert len(result) == 20
    assert all(num == 15 for num in result)

    # teardown test
    os.remove("log.sqlite")


def test_auto_gather_multi():
    # run example
    def is_unique_filename(str: str):
        return str.startswith("multi-log")

    n = 4

    log_files = [f for f in os.listdir() if is_unique_filename(f)]
    assert len(log_files) == 0  # no initial multi-log files

    filenames = []
    for i in range(n):
        name = f"multi-log-{i}.sqlite"
        filenames.append(name)
        create_log(name)

    log_files = [f for f in os.listdir() if is_unique_filename(f)]
    assert len(log_files) == n, "The logging files were not generated."

    # check schema

    # ensure quantity table exists
    db = make_wrapped_db(filenames, mangle=True, interactive=False)
    cur = db.q("select * from quantities")
    print("Quantity data:")
    result = list(cur)
    print(result)
    assert len(result) == 8

    # ensure warnings table exists
    db = make_wrapped_db(filenames, mangle=True, interactive=False)
    cur = db.q("select * from warnings")
    print("Warnings data:")
    result = list(cur)
    print(result)
    assert len(result) == 1

    # ensure logging in runs table exists
    db = make_wrapped_db(filenames, mangle=True, interactive=False)
    cur = db.q("select * from logging")
    print("Logging data:")
    result = list(cur)
    print(result)
    assert len(result) == n

    # check constant
    db = make_wrapped_db(filenames, mangle=True, interactive=False)
    cur = db.q("select * from runs")
    print("Constant data:")
    result = [row[0] for row in cur.description]
    print(result)
    assert len(result) == 11

    # teardown test
    for f in filenames:
        os.remove(f)
