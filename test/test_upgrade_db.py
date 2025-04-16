import os
import shutil
import sqlite3

import pytest

from logpyle import upgrade_db


@pytest.mark.parametrize("file", ["log_ungathered_v2.sqlite",
                                  "log_gathered_v2.sqlite",
                                  "log_ungathered_v3.sqlite",
                                  "log_gathered_v3.sqlite",
                                  "log_ungathered_v4.sqlite",
                                  "log_gathered_v4.sqlite",
                                  ])
@pytest.mark.parametrize("overwrite", [True, False])
def test_upgrade(file, overwrite):
    if overwrite:
        shutil.copy(".github/workflows/" + file, ".github/workflows/" + file + ".bak")

    filename = ".github/workflows/" + file
    suffix = "_pytest_upgrade"

    is_v2 = "v2" in file
    is_v3 = "v3" in file
    is_v4 = "v4" in file

    if is_v2:
        # ensure file is V2 and not newer
        conn = sqlite3.connect(filename)
        with pytest.raises(sqlite3.OperationalError):
            # should throw an exception because logging
            # should not exist in a V2 database
            print(list(conn.execute("select * from logging")))
        conn.close()

    if is_v3 or is_v4:
        # ensure it is at least V3
        conn = sqlite3.connect(filename)
        try:
            print(list(conn.execute("select unixtime from warnings")))
        except sqlite3.OperationalError as err:
            raise AssertionError(f"{filename} is not a V3 database") from err
        finally:
            conn.close()

    if is_v4:
        # ensure it is at least V4
        conn = sqlite3.connect(filename)
        try:
            print(list(conn.execute("select rank from constants")))
        except sqlite3.OperationalError as err:
            raise AssertionError(f"{filename} is not a V4 database") from err
        finally:
            conn.close()

    upgraded_name = upgrade_db.upgrade_db(filename, suffix, overwrite=overwrite)

    # ensure it is upgraded
    conn = sqlite3.connect(upgraded_name)
    try:
        print(list(conn.execute("select * from logging")))
        print(list(conn.execute("select * from constants")))
    except sqlite3.OperationalError as err:
        raise AssertionError(f"{upgraded_name} is not an upgraded database") from err
    finally:
        conn.close()
        os.remove(upgraded_name)

        if overwrite:
            shutil.copy(".github/workflows/" + file + ".bak",
                        ".github/workflows/" + file)
            os.remove(".github/workflows/" + file + ".bak")
