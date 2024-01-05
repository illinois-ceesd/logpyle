"""
Database Upgrade Functions
--------------------------------
.. autofunction:: upgrade_db
.. note::

   Currently, upgrades all schema versions to version 3.
   Upgrading from version <=1 is untested.

.. table:: Overview of known changes between schema versions

   ============== =========================== ==================================
   Schema version Logpyle version             Changes
   ============== =========================== ==================================
   0              pre v1 (``pytools.log``)    Initial version, no schema_version
                                              yet
   1              v1 -- v9 (``pytools.log``)  Added ``warnings`` table
   2              v10 -- 2023.1               Added ``warnings.rank`` column
   3              2023.2 --                   Added ``warnings.unixtime`` column
                                              and ``logging`` table
   ============== =========================== ==================================

"""
import logging
import shutil
import sqlite3
from pickle import dumps

logger = logging.getLogger(__name__)


def upgrade_conn(conn: sqlite3.Connection) -> sqlite3.Connection:
    from logpyle.runalyzer import is_gathered
    tmp = conn.execute("select * from warnings").description
    warning_columns = [col[0] for col in tmp]

    # check if the provided connection has been gathered
    gathered = is_gathered(conn)

    # ensure that warnings table has unixtime column
    if "unixtime" not in warning_columns:
        logger.info("Adding a unixtime column in the warnings table")
        conn.execute("""
            ALTER TABLE warnings
                ADD unixtime integer DEFAULT NULL;
                         """)

    # ensure that warnings table has rank column
    # nowhere to grab the rank of the process that generated
    # the warning
    if "rank" not in warning_columns:
        logger.info("Adding a rank column in the warnings table")
        conn.execute("""
            ALTER TABLE warnings
                ADD rank integer DEFAULT NULL;
                         """)

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

    schema_version = 3
    value = bytes(dumps(schema_version))
    if gathered:
        conn.execute("UPDATE runs SET schema_version=?",
                     (schema_version,))
    else:
        conn.execute("UPDATE constants SET value=? WHERE name='schema_version'",
                     (value,))

    return conn


def upgrade_db(
        dbfile: str, suffix: str, overwrite: bool
        ) -> None:
    """
    Upgrade a database file to the most recent format. If the
    `overwrite` parameter is True, it simply modifies the existing
    database and uses the same file name for the upgraded database.
    Otherwise, a new database is created with a separate filename
    by appending the given suffix to the original file's base name
    using `filename + suffix + "." + file_ext`.

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
        logger.info(f"Overwriting Database: {new_conn_name}")

    else:
        # separate the filename and the extension
        filename, file_ext = dbfile.rsplit(".", 1)

        new_conn_name = filename + suffix + "." + file_ext

        shutil.copy(dbfile, new_conn_name)

        new_conn = sqlite3.connect(new_conn_name)

        logger.info(f"Creating new Database: {new_conn_name}, a clone of {dbfile}")

    logger.info(f"Upgrading {new_conn_name} to schema version 3")

    new_conn = upgrade_conn(new_conn)

    if old_conn != new_conn:
        old_conn.close()

    new_conn.commit()
    new_conn.close()
