import os
import re
import sqlite3
from sqlite3 import Connection
from typing import Any, cast

from pytools.datatable import DataTable

from logpyle import LogManager

bool_feat_re = re.compile(r"^([a-z]+)(True|False)$")
int_feat_re = re.compile(r"^([a-z]+)([0-9]+)$")
real_feat_re = re.compile(r"^([a-z]+)([0-9]+\.?[0-9]*)$")
str_feat_re = re.compile(r"^([a-z]+)([A-Z][A-Za-z_0-9]+)$")

sqlite_keywords = """
    abort action add after all alter analyze and as asc attach
    autoincrement before begin between by cascade case cast check
    collate column commit conflict constraint create cross current_date
    current_time current_timestamp database default deferrable deferred
    delete desc detach distinct drop each else end escape except
    exclusive exists explain fail for foreign from full glob group
    having if ignore immediate in index indexed initially inner insert
    instead intersect into is isnull join key left like limit match
    natural no not notnull null of offset on or order outer plan pragma
    primary query raise references regexp reindex release rename
    replace restrict right rollback row savepoint select set table temp
    temporary then to transaction trigger union unique update using
    vacuum values view virtual when where""".split()


def parse_dir_feature(feat: str, number: int) \
                        -> tuple[str | Any, str, str | Any]:
    bool_match = bool_feat_re.match(feat)
    if bool_match is not None:
        return (bool_match.group(1), "integer", int(bool_match.group(2) == "True"))
    int_match = int_feat_re.match(feat)
    if int_match is not None:
        return (int_match.group(1), "integer", float(int_match.group(2)))
    real_match = real_feat_re.match(feat)
    if real_match is not None:
        return (real_match.group(1), "real", float(real_match.group(2)))
    str_match = str_feat_re.match(feat)
    if str_match is not None:
        return (str_match.group(1), "text", str_match.group(2))
    return (f"dirfeat{number}", "text", feat)


def larger_sql_type(type_a: str | None, type_b: str | None) -> str | None:
    assert type_a in [None, "text", "real", "integer"]
    assert type_b in [None, "text", "real", "integer"]

    if type_a is None:
        return type_b
    if type_b is None:
        return type_a
    if "text" in [type_a, type_b]:
        return "text"
    if "real" in [type_a, type_b]:
        return "real"
    assert type_a == type_b == "integer"
    return "integer"


def sql_type_and_value(value: Any) \
                        -> tuple[str | None, int | float | str | None]:
    if value is None:
        return None, None
    elif isinstance(value, bool):
        return "integer", int(value)
    elif isinstance(value, int):
        return "integer", value
    elif isinstance(value, float):
        return "real", value
    else:
        return "text", str(value)


def sql_type_and_value_from_str(value: str) \
                        -> tuple[str | None, int | float | str | None]:
    if value == "None":
        return None, None
    elif value in ["True", "False"]:
        return "integer", value == "True"
    else:
        try:
            return "integer", int(value)
        except ValueError:
            pass
        try:
            return "real", float(value)
        except ValueError:
            pass
        return "text", str(value)


class FeatureGatherer:
    def __init__(self, features_from_dir: bool = False,
                 features_file: str | None = None) -> None:
        self.features_from_dir = features_from_dir

        self.dir_to_features = {}
        if features_file is not None:
            for line in open(features_file).readlines():
                colon_idx = line.find(":")
                assert colon_idx != -1

                entries = [val.strip() for val in line[colon_idx + 1:].split(",")]
                features = []
                for entry in entries:
                    equal_idx = entry.find("=")
                    assert equal_idx != -1
                    features.append((entry[:equal_idx],
                                     *sql_type_and_value_from_str(
                                         entry[equal_idx + 1:])))

                self.dir_to_features[line[:colon_idx]] = features

    def get_db_features(self, dbname: str, logmgr: LogManager) -> list[Any]:
        from os.path import dirname
        dn = dirname(dbname)

        features = self.dir_to_features.get(dn, [])[:]

        if self.features_from_dir:
            features.extend(parse_dir_feature(feat, i)
                    for i, feat in enumerate(dn.split("-")))

        for name, value in logmgr.constants.items():
            features.append((name, *sql_type_and_value(value)))

        return features


def scan(fg: FeatureGatherer, dbnames: list[str],  # noqa: C901
         progress: bool = True) -> tuple[dict[str, Any], dict[str, int]]:
    features: dict[str, Any] = {}
    dbname_to_run_id = {}
    uid_to_run_id: dict[str, int] = {}
    next_run_id = 1

    from pytools import ProgressBar
    if progress:
        pb = ProgressBar("Scanning...",  # type: ignore[no-untyped-call]
                         len(dbnames))

    for dbname in dbnames:
        try:
            logmgr = LogManager(dbname, "r")
        except Exception:
            print(f"Trouble with file '{dbname}'")
            raise

        unique_run_id = cast(str, logmgr.constants.get("unique_run_id"))
        run_id = uid_to_run_id.get(unique_run_id)

        if run_id is None:
            run_id = next_run_id
            next_run_id += 1

            if unique_run_id is not None:
                uid_to_run_id[unique_run_id] = run_id

        dbname_to_run_id[dbname] = run_id

        if progress:
            pb.progress()  # type: ignore[no-untyped-call]

        for fname, ftype, _fvalue in fg.get_db_features(dbname, logmgr):
            if fname in features:
                features[fname] = larger_sql_type(ftype, features[fname])
            else:
                if ftype is None:
                    ftype = "text"
                features[fname] = ftype

        logmgr.close()

    if progress:
        pb.finished()  # type: ignore[no-untyped-call]

    return features, dbname_to_run_id


def make_name_map(map_str: str) -> dict[str, str]:
    import re
    result: dict[str, str] = {}

    if not map_str:
        return result

    map_re = re.compile(r"^([a-z_A-Z0-9]+)=([a-z_A-Z0-9]+)$")
    for fmap_entry in map_str.split(","):
        match = map_re.match(fmap_entry)
        if not (match and match.group(1) and match.group(2)):
            raise RuntimeError(
                    "Arguments to -m should have the form F1=FNAME1,F2=FNAME2,...")
        result[match.group(1)] = match.group(2)

    return result


def _normalize_types(x: Any) -> Any:
    # get rid of numpy types
    if isinstance(x, int):
        return int(x)
    if isinstance(x, float):
        return float(x)
    return x


def gather_multi_file(outfile: str, infiles: list[str], fmap: dict[str, str],  # noqa: C901
                      qmap: dict[str, str], fg: FeatureGatherer,
                      features: dict[str, Any],
                      dbname_to_run_id: dict[str, int]) -> sqlite3.Connection:
    from pytools import ProgressBar
    pb = ProgressBar("Importing...", len(infiles))  # type: ignore[no-untyped-call]

    feature_col_name_map = {}
    for fname in features:
        tgt_name = fmap.get(fname, fname)

        if tgt_name.lower() in sqlite_keywords:
            feature_col_name_map[fname] = tgt_name + "_"
        else:
            feature_col_name_map[fname] = tgt_name

    if os.path.exists(outfile):  # pragma: no cover
        print(f"Error: output file '{outfile}' already exists, exiting.")
        import sys
        sys.exit(1)

    import sqlite3
    db_conn = sqlite3.connect(outfile)
    run_columns = [
            "id integer primary key",
            "dirname text",
            "filename text",
            ] + [f"{feature_col_name_map[fname]} {ftype}"
                    for fname, ftype in features.items()]
    db_conn.execute("create table runs ({})".format(",".join(run_columns)))
    db_conn.execute("create index runs_id on runs (id)")

    # Caveat: the next three tables need to match the tables in _set_up_schema,
    # plus the 'id'/'run_id' columns.
    db_conn.execute("""create table quantities (
            id integer primary key,
            name text,
            unit text,
            description text,
            rank_aggregator text
            )""")

    db_conn.execute("""
      create table warnings (
        run_id integer,
        rank integer,
        step integer,
        unixtime integer,
        message text,
        category text,
        filename text,
        lineno integer
        )""")

    db_conn.execute("""
      create table logging (
        run_id integer,
        rank integer,
        step integer,
        unixtime integer,
        level text,
        message text,
        filename text,
        lineno integer
        )""")

    created_tables = set()

    from os.path import basename, dirname

    written_run_ids = set()

    for dbname in infiles:
        pb.progress()  # type: ignore[no-untyped-call]

        run_id = dbname_to_run_id[dbname]

        logmgr = LogManager(dbname, "r")

        if run_id not in written_run_ids:
            dbfeatures = fg.get_db_features(dbname, logmgr)
            qry = "insert into runs ({}) values ({})".format(
                ",".join(["id", "dirname", "filename"]
                    + [feature_col_name_map[f[0]] for f in dbfeatures]),
                ",".join("?" * (len(dbfeatures) + 3)))
            db_conn.execute(qry,
                    [run_id, dirname(dbname), basename(dbname)]
                    + [_normalize_types(f[2]) for f in dbfeatures])

            written_run_ids.add(run_id)

        def transfer_data_table_multi(db_conn: Connection, tbl_name: str,
                                      data_table: DataTable) -> None:
            my_data = [(run_id, *d) for d in data_table.data]  # noqa: B023

            db_conn.executemany(f"insert into {tbl_name} (%s) values (%s)" %
                ("run_id,"
                    + ", ".join(data_table.column_names),
                    ", ".join("?" * (len(data_table.column_names) + 1))),
                my_data)

        transfer_data_table_multi(db_conn, "warnings", logmgr.get_warnings())
        transfer_data_table_multi(db_conn, "logging", logmgr.get_logging())

        for qname, qdat in logmgr.quantity_data.items():
            tgt_qname = qmap.get(qname, qname)

            if tgt_qname not in created_tables:
                created_tables.add(tgt_qname)
                db_conn.execute(f"create table {tgt_qname} ("
                  "run_id integer, step integer, rank integer, value real)")

                db_conn.execute(
                    f"create index {tgt_qname}_main on {tgt_qname} (run_id,step,rank)")

                agg = qdat.default_aggregator
                try:
                    agg = agg.__name__  # type: ignore[union-attr, assignment]
                except AttributeError:
                    if agg is not None:
                        agg = str(agg)  # type: ignore[assignment]

                db_conn.execute("insert into quantities "
                        "(name,unit,description,rank_aggregator)"
                        "values (?,?,?,?)",
                        (tgt_qname, qdat.unit, qdat.description, agg))

            cursor = logmgr.db_conn.execute(
                    f"select {run_id},step,rank,value from {qname}")
            db_conn.executemany(f"insert into {tgt_qname} values (?,?,?,?)",
                    cursor)
        logmgr.close()
    pb.finished()  # type: ignore[no-untyped-call]

    db_conn.commit()
    return db_conn
