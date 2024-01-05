import os
import sqlite3

import pytest

from logpyle import upgrade_db


def test_upgrade_v2_v3():
    path = ".github/workflows/log_gathered_v2.sqlite"
    suffix = "_pytest_upgrade"
    filename, file_ext = path.rsplit(".", 1)

    # ensure it is V2
    conn = sqlite3.connect(filename + "." + file_ext)
    with pytest.raises(sqlite3.OperationalError):
        # should throw an exception because logging
        # should not exist in a V2 database
        print(list(conn.execute("select * from logging")))
    conn.close()

    upgrade_db.upgrade_db(path, suffix, False)

    # ensure it is V3
    upgraded_name = filename + suffix + "." + file_ext
    conn = sqlite3.connect(upgraded_name)
    try:
        print(list(conn.execute("select * from logging")))
    except sqlite3.OperationalError:
        os.remove(upgraded_name)
        raise AssertionError(f"{upgraded_name} is not a v3 database")
    conn.close()
    os.remove(upgraded_name)
