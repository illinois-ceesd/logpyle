"""
Database Upgrade Functions
--------------------------------
.. autofunction:: upgrade_db
.. note::

   Currently, upgrades all schema versions to version 4.
   Upgrading from versions <=1 is untested.

.. table:: Overview of known changes between schema versions

   ============== =========================== ======================================
   Schema version Logpyle version             Changes
   ============== =========================== ======================================
   0              pre v1 (``pytools.log``)    Initial version, no ``schema_version``
                                              yet.
   1              v1 -- v9 (``pytools.log``)  Added ``warnings`` table.
   2              v10 -- 2023.1               Added ``warnings.rank`` column.
   3              2023.2 -- 2025.0            Added ``warnings.unixtime`` column
                                              and ``logging`` table.
   4              2025.1 --                   Added ``constants.rank`` column
                                              and ``constants`` gathered table.
   ============== =========================== ======================================

"""
import logging
import shutil
import sqlite3
from pickle import dumps

logger = logging.getLogger(__name__)


def upgrade_conn(conn: sqlite3.Connection) -> sqlite3.Connection:
    from logpyle.runalyzer import is_gathered
    gathered = is_gathered(conn)

    # {{{ warnings table

    tmp = conn.execute("select * from warnings").description
    warning_columns = [col[0] for col in tmp]

    # ensure that the warnings table has the 'unixtime' column
    if "unixtime" not in warning_columns:
        logger.info("Adding a unixtime column in the warnings table")
        conn.execute("""
            ALTER TABLE warnings
                ADD unixtime integer DEFAULT NULL;
                         """)

    # ensure that the warnings table has the 'rank' column
    # nowhere to grab the rank of the process that generated
    # the warning
    if "rank" not in warning_columns:  # pragma: no cover
        logger.info("Adding a rank column in the warnings table")
        conn.execute("""
            ALTER TABLE warnings
                ADD rank integer DEFAULT NULL;
                         """)

    # }}}

    # {{{ constants table

    if not gathered:
        tmp = conn.execute("select * from constants").description
        constants_columns = [col[0] for col in tmp]

        # ensure that the constants table has the 'rank' column
        if "rank" not in constants_columns:
            logger.info("Adding a rank column in the constants table")
            conn.execute("""
                ALTER TABLE constants
                    ADD rank integer DEFAULT NULL;
                             """)
    else:
        # transfer columns from runs table to rows in constants table
        conn.execute("""
                        CREATE TABLE IF NOT EXISTS constants (
                            run_id INTEGER,
                            rank INTEGER,
                            name TEXT,
                            value BLOB)""")
        # Transfer columns from runs table to rows in constants table
        # Dynamically handle unknown column names
        tmp = conn.execute("PRAGMA table_info(runs)").fetchall()
        columns = [col[1] for col in tmp if col[1] not in ("id", "dirname", "filename")]

        # In schema_version < 4, we can not determine the rank after execution finished
        rank = dumps(None)

        # Insert columns from the runs table as rows into the constants table
        for column in columns:
            conn.execute(f"""
                INSERT INTO constants (run_id, rank, name, value)
                SELECT id, ?, ?, {column}
                FROM runs
            """, (rank, column))

        # Remove transferred columns from the runs table
        logger.info("Removing transferred columns from the runs table")
        remaining_columns = [col[1] for col in tmp if col[1] not in columns]

        # Create a temporary table with the remaining columns
        conn.execute(f"""
            CREATE TABLE runs_temp AS
            SELECT {', '.join(remaining_columns)}
            FROM runs
        """)

        # Drop the original runs table
        conn.execute("DROP TABLE runs")

        # Rename the temporary table to the original table's name
        conn.execute("ALTER TABLE runs_temp RENAME TO runs")

    # }}}

    # {{{ logging table

    tables = [col[0] for col in conn.execute("""
                    SELECT name
                    FROM sqlite_master
                    WHERE type='table'
                       """)]

    logger.info("Ensuring a logging table exists")
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

    # }}}

    # {{{ update schema_version

    schema_version = 4
    value = bytes(dumps(schema_version))
    conn.execute("UPDATE constants SET value=? WHERE name='schema_version'",
                     (value,))

    # }}}

    conn.commit()
    return conn


def upgrade_db(
        dbfile: str, suffix: str, overwrite: bool = False
        ) -> str:
    """
    Upgrade a database file to the most recent format. If the
    `overwrite` parameter is True, it simply modifies the existing
    database and uses the same file name for the upgraded database.
    Otherwise, a new database is created with a separate filename
    by appending the given suffix to the original file's base name
    using `filename + suffix + "." + file_ext`.

    :arg dbfile: The path and filename for the database to be upgraded.
    :arg suffix: A suffix to be appended to the filename for the upgraded
        database.
    :arg overwrite: A boolean value indicating whether to overwrite the
        original database or not. If *True*, *suffix* is ignored.

    :returns: The filename of the upgraded database.
    """

    # original db files
    old_conn = sqlite3.connect(dbfile)

    if overwrite:
        # simply perform modifications on old connection
        new_conn_name = dbfile
        new_conn = old_conn
        logger.info(f"Overwriting Database: {new_conn_name}")

    else:
        # separate the filename and the extension
        filename, file_ext = dbfile.rsplit(".", 1)

        new_conn_name = filename + suffix + "." + file_ext

        shutil.copy(dbfile, new_conn_name)

        new_conn = sqlite3.connect(new_conn_name)

        logger.info(f"Creating new Database: {new_conn_name}, a clone of {dbfile}")

    logger.info(f"Upgrading {new_conn_name} to schema version 4")

    new_conn = upgrade_conn(new_conn)

    if old_conn != new_conn:
        old_conn.close()

    new_conn.commit()
    new_conn.close()

    return new_conn_name
