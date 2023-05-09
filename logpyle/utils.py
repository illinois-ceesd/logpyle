# {{{ sqlite3 thread safety

def get_sqlite3_thread_safety_level() -> int:
    """Return the full threadsafety value."""
    import sqlite3

    # Map value from SQLite's THREADSAFE to Python's DBAPI 2.0
    # threadsafety attribute.
    sqlite_threadsafe2python_dbapi = {0: 0, 2: 1, 1: 3}
    conn = sqlite3.connect(":memory:")
    threadsafety = conn.execute(
        """
select * from pragma_compile_options
where compile_options like 'THREADSAFE=%'
"""
    ).fetchone()[0]
    conn.close()

    threadsafety_value = int(threadsafety.split("=")[1])
    threadsafety_value_db = sqlite_threadsafe2python_dbapi[threadsafety_value]

    import sys
    if sys.version_info < (3, 11):
        assert threadsafety_value == 1
    else:
        assert threadsafety_value_db == sqlite3.threadsafety

    return threadsafety_value_db


def is_sqlite3_fully_threadsafe() -> bool:
    if get_sqlite3_thread_safety_level() == 3:
        return True
    else:
        return False


# }}}
