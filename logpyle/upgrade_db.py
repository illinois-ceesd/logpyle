"""
Database Upgrade Functions
--------------------------------
.. autofunction:: upgrade_db
"""
import shutil
import sqlite3


def upgrade_conn(conn: sqlite3.Connection) -> sqlite3.Connection:
    from logpyle.runalyzer import is_gathered
    tmp = conn.execute("select * from warnings").description
    warning_columns = [col[0] for col in tmp]

    # check if the provided connection has been gathered
    gathered = is_gathered(conn)

    # ensure that warnings table has unixtime column
    if "unixtime" not in warning_columns:
        print("Adding a unixtime column in the warnings table")
        conn.execute("""
            ALTER TABLE warnings
                ADD unixtime integer DEFAULT NULL;
                         """)

    # ensure that warnings table has rank column
    # nowhere to grab the rank of the process that generated
    # the warning
    if "rank" not in warning_columns:
        print("Adding a rank column in the warnings table")
        conn.execute("""
            ALTER TABLE warnings
                ADD rank integer DEFAULT NULL;
                         """)

    tables = [col[0] for col in conn.execute("""
                    SELECT name
                    FROM sqlite_master
                    WHERE type='table'
                       """)]

    print("Ensuring a logging table exists")
    if "logging" not in tables:
        conn.execute("""
            CREATE TABLE logging (
                rank integer,
                step integer,
                unixtime integer,
                level text,
                message text,
                filename text,
                lineno integer
                )""")
        if gathered:
            conn.execute("""
                ALTER TABLE logging
                ADD run_id integer;
                             """)

    return conn


def upgrade_db(
        dbfile: str, suffix: str, overwrite: bool
        ) -> None:
    """

    The function first connects to the original database . If the
    `overwrite` parameter is True, it simply modifies the existing
    database and uses the same file name for the upgraded database.
    Otherwise, a new database is created with a separate filename
    by appending the given suffix to the original file's base name
    using `filename + suffix + "." + file_ext`.

    Next, the function prints a message indicating whether it is
    overwriting or creating a new database and then proceeds to
    upgrade the database schema version to 3.

    Parameters
    ----------
    dbfile
      A database file path

    suffix
      a suffix to be appended to the filename for the
      upgraded database

    overwrite
      a boolean value indicating
      whether to overwrite the original database or not
    """

    # original db files
    old_conn = sqlite3.connect(dbfile)

    if overwrite:
        # simply perform modifications on old connection
        new_conn_name = dbfile
        new_conn = old_conn
        print(f"Overwriting Database: {new_conn_name}")

    else:
        # seperate the filename and the extention
        filename, file_ext = dbfile.rsplit(".", 1)

        new_conn_name = filename + suffix + "." + file_ext

        shutil.copy(dbfile, new_conn_name)

        new_conn = sqlite3.connect(new_conn_name)

        print(f"Creating new Database: {new_conn_name}, a clone of {dbfile}")

    print(f"Upgrading {new_conn_name} to schema version 3")

    new_conn = upgrade_conn(new_conn)

    if old_conn != new_conn:
        old_conn.close()

    new_conn.commit()
    new_conn.close()
