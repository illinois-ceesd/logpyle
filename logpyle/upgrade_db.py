import shutil
import sqlite3


def upgrade_conn(conn: sqlite3.Connection) -> sqlite3.Connection:
    tmp = conn.execute("select * from warnings").description
    warning_columns = [col[0] for col in tmp]

    # check if any of the provided files have been gathered
    gathered = False
    # get a list of tables with the name of 'runs'
    res = list(conn.execute("""
                        SELECT name
                        FROM sqlite_master
                        WHERE type='table' AND name='runs'
                                      """))
    if len(res) == 1:
        gathered = True

    # ensure that warnings table has unixtime column
    if ("unixtime" not in warning_columns):
        print("Adding a unixtime column in the warnings table")
        conn.execute("""
                         ALTER TABLE warnings
                            ADD unixtime integer DEFAULT NULL;
                         """)

    # ensure that warnings table has rank column
    # nowhere to grab the rank of the process that generated
    # the warning
    if ("rank" not in warning_columns):
        print("Adding a rank column in the warnings table")
        conn.execute("""
                         ALTER TABLE warnings
                            ADD rank integer DEFAULT NULL;
                         """)

    print("Ensuring a logging table exists")
    if gathered:
        conn.execute("""
          CREATE TABLE IF NOT EXISTS logging (
            run_id integer,
            rank integer,
            step integer,
            unixtime integer,
            level text,
            message text,
            filename text,
            lineno integer
            )""")
    else:
        conn.execute("""
          CREATE TABLE IF NOT EXISTS logging (
            rank integer,
            step integer,
            unixtime integer,
            level text,
            message text,
            filename text,
            lineno integer
            )""")

    return conn


def upgrade_db(
        dbfile: str, suffix: str, overwrite: bool
        ) -> sqlite3.Connection:

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

    old_conn.close()

    return new_conn
