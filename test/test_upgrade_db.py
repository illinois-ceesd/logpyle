import os
import sqlite3

import pytest

from logpyle import upgrade_db


@pytest.mark.parametrize("file", ["log_ungathered_v2.sqlite",
                                  "log_gathered_v2.sqlite"])
def test_upgrade_v2_v3(file):
    filename = ".github/workflows/" + file
    suffix = "_pytest_upgrade"

    # ensure it is V2
    conn = sqlite3.connect(filename)
    with pytest.raises(sqlite3.OperationalError):
        # should throw an exception because logging
        # should not exist in a V2 database
        print(list(conn.execute("select * from logging")))
    conn.close()

    upgraded_name = upgrade_db.upgrade_db(filename, suffix, overwrite=False)

    # ensure it is V3
    conn = sqlite3.connect(upgraded_name)
    try:
        print(list(conn.execute("select * from logging")))
    except sqlite3.OperationalError as err:
        raise AssertionError(f"{upgraded_name} is not a v3 database") from err
    finally:
        conn.close()
        os.remove(upgraded_name)
