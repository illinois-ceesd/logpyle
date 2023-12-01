import sqlite3

from logpyle import (upgrade_db)


def test_upgrade_v2_v3():
    path = ".github/workflows/log_gathered_v2.sqlite"
    suffix = "_pytest_upgrade"
    # print(path.rsplit(".", 1))
    # assert False
    filename, file_ext = path.rsplit(".", 1)

    # ensure it is V2
    conn = sqlite3.connect(filename + "." + file_ext)
    try:
        # should throw an exception because logging
        # should not exist in a V2 database
        print([ele for ele in conn.execute("select * from logging")])
        assert False, f"{filename} is a v3 database"
    except sqlite3.OperationalError:
        pass  # v2 should not have a logging table
    conn.close()

    upgrade_db.upgrade_db(path, suffix, False)

    # ensure it is V3
    conn = sqlite3.connect(filename + suffix + "." + file_ext)
    try:
        print([ele for ele in conn.execute("select * from logging")])
    except sqlite3.OperationalError:
        assert False, f"{filename} is not a v3 database"
    conn.close()
