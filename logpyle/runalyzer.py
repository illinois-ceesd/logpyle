#! /usr/bin/env python

import code
import sqlite3

try:
    import readline
    import rlcompleter  # noqa: F401
    HAVE_READLINE = True
except ImportError:
    HAVE_READLINE = False


import logging

logger = logging.getLogger(__name__)

from dataclasses import dataclass
from itertools import product
from sqlite3 import Connection, Cursor
from typing import (Any, Callable, Dict, Generator, List, Optional, Sequence, Set,
                    Tuple, Type, Union)

from pytools import Table


@dataclass(frozen=True)
class PlotStyle:
    dashes: Tuple[int, ...]
    color: str


PLOT_STYLES = [
        PlotStyle(dashes=dashes, color=color)
        for dashes, color in product(
            [(), (12, 2), (4, 2),  (2, 2), (2, 8)],
            ["blue", "green", "red", "magenta", "cyan"],
            )]


class RunDB:
    def __init__(self, db: Connection, interactive: bool) -> None:
        self.db = db
        self.interactive = interactive
        self.rank_agg_tables: Set[Tuple[str, Callable[..., Any]]] = set()

    def __del__(self) -> None:
        self.db.close()

    def q(self, qry: str, *extra_args: Any) -> Cursor:
        return self.db.execute(self.mangle_sql(qry), extra_args)

    def mangle_sql(self, qry: str) -> str:
        return qry

    def get_rank_agg_table(self, qty: str,
                           rank_aggregator: Callable[..., Any]) -> str:
        tbl_name = f"rankagg_{rank_aggregator}_{qty}"

        if (qty, rank_aggregator) in self.rank_agg_tables:
            return tbl_name

        logger.info("Building temporary rank aggregation table {tbl_name}.")

        self.db.execute("create temporary table %s as "
                "select run_id, step, %s(value) as value "
                "from %s group by run_id,step" % (
                    tbl_name, rank_aggregator, qty))
        self.db.execute("create index %s_run_step on %s (run_id,step)"
                % (tbl_name, tbl_name))
        self.rank_agg_tables.add((qty, rank_aggregator))
        return tbl_name

    def scatter_cursor(self, cursor: Cursor, labels: Optional[List[str]] = None,
                       *args: Any, **kwargs: Any) -> None:
        import matplotlib.pyplot as plt

        data_args = tuple(zip(*list(cursor)))
        plt.scatter(*(data_args + args), **kwargs)

        if isinstance(labels, list) and len(labels) == 2:
            plt.xlabel(labels[0])
            plt.ylabel(labels[1])
        elif labels is not None:
            raise TypeError("The 'labels' parameter must be a list with two"
                            "elements.")

        if self.interactive:
            plt.show()

    def plot_cursor(self, cursor: Cursor, labels: Optional[List[str]] = None,
                    *args: Any, **kwargs: Any) -> None:
        from matplotlib.pyplot import legend, plot, show

        auto_style = kwargs.pop("auto_style", True)

        if len(cursor.description) == 2:
            if auto_style:
                style = PLOT_STYLES[0]
                kwargs["dashes"] = style.dashes
                kwargs["color"] = style.color

            x, y = list(zip(*list(cursor)))
            p = plot(x, y, *args, **kwargs)

            if isinstance(labels, list) and len(labels) == 2:
                p[0].axes.set_xlabel(labels[0])
                p[0].axes.set_ylabel(labels[1])
            elif labels is not None:
                raise TypeError("The 'labels' parameter must be a list with two"
                                " elements.")

        elif len(cursor.description) > 2:
            small_legend = kwargs.pop("small_legend", True)

            def format_label(kv_pairs: Sequence[Tuple[str, Any]]) -> str:
                return " ".join(f"{column}:{value}"
                            for column, value in kv_pairs)
            format_label = kwargs.pop("format_label", format_label)

            def do_plot(x: List[float], y: List[float],
                        row_rest: Tuple[Any, ...]) -> None:
                my_kwargs = kwargs.copy()
                style = PLOT_STYLES[style_idx[0] % len(PLOT_STYLES)]
                if auto_style:
                    my_kwargs.setdefault("dashes", style.dashes)
                    my_kwargs.setdefault("color", style.color)

                my_kwargs.setdefault("label",
                        format_label(list(zip(
                            (col[0] for col in cursor.description[2:]),
                            row_rest))))

                plot(x, y, *args, hold=True, **my_kwargs)
                style_idx[0] += 1

            style_idx = [0]
            for x, y, rest in split_cursor(cursor):
                do_plot(x, y, rest)  # type: ignore[arg-type]

            if small_legend:
                from matplotlib.font_manager import FontProperties
                legend(pad=0.04, prop=FontProperties(size=8), loc="best",
                        labelsep=0)
        else:
            raise ValueError("invalid number of columns")

        if self.interactive:
            show()

    def print_cursor(self, cursor: Cursor) -> None:
        print(table_from_cursor(cursor))


def split_cursor(cursor: Cursor) -> Generator[
        Tuple[List[Any], List[Any], Optional[Tuple[Any, ...]]], None, None]:

    x: List[Any] = []
    y: List[Any] = []
    last_rest = None
    for row in cursor:
        row_tuple = tuple(row)
        row_rest = row_tuple[2:]

        if last_rest is None:
            last_rest = row_rest

        if row_rest != last_rest:
            yield x, y, last_rest
            del x[:]
            del y[:]

            last_rest = row_rest

        x.append(row_tuple[0])
        y.append(row_tuple[1])
    if x:
        yield x, y, last_rest


def table_from_cursor(cursor: Cursor) -> Table:
    tbl = Table()
    tbl.add_row(tuple([column[0] for column in cursor.description]))
    for row in cursor:
        tbl.add_row(row)
    return tbl


class MagicRunDB(RunDB):
    def mangle_sql(self, qry: str) -> str:
        up_qry = qry.upper()
        if "FROM" in up_qry and "$$" not in up_qry:
            return qry

        magic_columns = set()
        import re

        # should be: re.Match[Any]
        def replace_magic_column(match: Any) -> str:
            qty_name = match.group(1)
            rank_aggregator = match.group(2)

            if rank_aggregator is not None:
                rank_aggregator = rank_aggregator[1:]
                magic_columns.add((qty_name, rank_aggregator))
                return f"{rank_aggregator}_{qty_name}.value AS {qty_name}"
            else:
                magic_columns.add((qty_name, None))
                return "%s.value AS %s" % (qty_name, qty_name)

        magic_column_re = re.compile(r"\$([a-zA-Z][A-Za-z0-9_]*)(\.[a-z]*)?")
        qry, _ = magic_column_re.subn(replace_magic_column, qry)

        other_clauses = [  # noqa: F841
                "UNION",  "INTERSECT", "EXCEPT", "WHERE", "GROUP",
                "HAVING", "ORDER", "LIMIT", ";"]

        from_clause = "from runs "
        last_tbl = None
        for tbl, rank_aggregator in magic_columns:
            if rank_aggregator is not None:
                full_tbl = f"{rank_aggregator}_{tbl}"
                full_tbl_src = "{} as {}".format(
                        self.get_rank_agg_table(tbl, rank_aggregator),
                        full_tbl)

                if last_tbl is not None:
                    addendum = f" and {last_tbl}.step = {full_tbl}.step"
                else:
                    addendum = ""
            else:
                full_tbl = tbl
                full_tbl_src = tbl

                if last_tbl is not None:
                    addendum = " and {}.step = {}.step and {}.rank={}.rank".format(
                            last_tbl, full_tbl, last_tbl, full_tbl)
                else:
                    addendum = ""

            from_clause += " inner join {} on ({}.run_id = runs.id{}) ".format(
                    full_tbl_src, full_tbl, addendum)
            last_tbl = full_tbl

        def get_clause_indices(qry: str) -> Dict[str, int]:
            other_clauses = ["UNION",  "INTERSECT", "EXCEPT", "WHERE", "GROUP",
                    "HAVING", "ORDER", "LIMIT", ";"]

            result = {}
            up_qry = qry.upper()
            for clause in other_clauses:
                clause_match = re.search(r"\b%s\b" % clause, up_qry)
                if clause_match is not None:
                    result[clause] = clause_match.start()

            return result

        # add 'from'
        if "$$" in qry:
            qry = qry.replace("$$", " %s " % from_clause)
        else:
            clause_indices = get_clause_indices(qry)

            if not clause_indices:
                qry = qry+" "+from_clause
            else:
                first_clause_idx = min(clause_indices.values())
                qry = (
                        qry[:first_clause_idx]
                        + from_clause
                        + qry[first_clause_idx:])

        return qry


def make_runalyzer_symbols(db: RunDB) \
        -> Dict[str, Union[RunDB, str, None, Callable[..., Any]]]:
    return {
            "__name__": "__console__",
            "__doc__": None,
            "db": db,
            "mangle_sql": db.mangle_sql,
            "q": db.q,
            "dbplot": db.plot_cursor,
            "dbscatter": db.scatter_cursor,
            "dbprint": db.print_cursor,
            "split_cursor": split_cursor,
            "table_from_cursor": table_from_cursor,
            }


class RunalyzerConsole(code.InteractiveConsole):
    def __init__(self, db: RunDB) -> None:
        self.db = db
        code.InteractiveConsole.__init__(self,
                make_runalyzer_symbols(db))

        try:
            import numpy  # noqa: F401
            self.runsource("from numpy import *")
        except ImportError:
            pass

        try:
            import matplotlib.pyplot  # noqa
            self.runsource("from matplotlib.pyplot import *")
        except ImportError:
            pass
        except RuntimeError:
            pass

        if HAVE_READLINE:
            import atexit
            import os

            histfile = os.path.join(os.environ["HOME"], ".runalyzerhist")
            if os.access(histfile, os.R_OK):
                readline.read_history_file(histfile)
            atexit.register(readline.write_history_file, histfile)
            readline.parse_and_bind("tab: complete")

        self.last_push_result = False

    def push(self, cmdline: str) -> bool:
        if cmdline.startswith("."):
            try:
                self.execute_magic(cmdline)
            except Exception:
                import traceback
                traceback.print_exc()
        else:
            self.last_push_result = code.InteractiveConsole.push(self, cmdline)

        return self.last_push_result

    def execute_magic(self, cmdline: str) -> None:
        cmd_end = cmdline.find(" ")
        if cmd_end == -1:
            cmd = cmdline[1:]
            args = ""
        else:
            cmd = cmdline[1:cmd_end]
            args = cmdline[cmd_end+1:]

        if cmd == "help":
            print("""
Commands:
 .help        show this help message
 .q SQL       execute a (potentially mangled) query
 .constants   show a list of (constant) run properties
 .quantities  show a list of time-dependent quantities
 .warnings    show a list of warnings
 .logging     show a list of logging messages

Plotting:
 .plot SQL    plot results of (potentially mangled) query.
              result sets can be (x,y) or (x,y,descr1,descr2,...),
              in which case a new plot will be started for each
              tuple (descr1, descr2, ...)
 .scatter SQL make scatterplot results of (potentially mangled) query.
              result sets can have between two and four columns
              for (x,y,size,color).

SQL mangling, if requested ("MagicSQL"):
    select $quantity where pred(feature)

Custom SQLite aggregates:
    stddev, var, norm1, norm2

Available Python symbols:
    db: the SQLite database
    mangle_sql(query_str): mangle the SQL query string query_str
    q(query_str): get db cursor for mangled query_str
    dbplot(cursor): plot result of cursor
    dbscatter(cursor): make scatterplot result of cursor
    dbprint(cursor): print result of cursor
    split_cursor(cursor): x,y,data gather that .plot uses internally
    table_from_cursor(cursor): Create a printable table from a cursor
""")
        elif cmd == "q":
            self.db.print_cursor(self.db.q(args))

        elif cmd == "runprops" or cmd == "constants":
            cursor = self.db.db.execute("select * from runs")
            columns = [column[0] for column in cursor.description]
            columns.sort()
            for col in columns:
                print(col)
        elif cmd == "quantities":
            self.db.print_cursor(self.db.q("select * from quantities order by name"))
        elif cmd == "warnings":
            self.db.print_cursor(self.db.q("select * from warnings"))
        elif cmd == "logging":
            self.db.print_cursor(self.db.q("select * from logging"))
        elif cmd == "title":
            from pylab import title
            title(args)
        elif cmd == "plot":
            cursor = self.db.db.execute(self.db.mangle_sql(args))
            columnnames = [column[0] for column in cursor.description]
            self.db.plot_cursor(cursor, labels=columnnames)
        elif cmd == "scatter":
            cursor = self.db.db.execute(self.db.mangle_sql(args))
            columnnames = [column[0] for column in cursor.description]
            self.db.scatter_cursor(cursor, labels=columnnames)
        else:
            print("invalid magic command")


# {{{ custom aggregates

from pytools import VarianceAggregator  # noqa: E402


class Variance(VarianceAggregator):
    def __init__(self) -> None:
        VarianceAggregator.__init__(self,  # type: ignore[no-untyped-call]
                                    entire_pop=True)


class StdDeviation(Variance):
    def finalize(self) -> Optional[float]:
        result = Variance.finalize(self)  # type: ignore[no-untyped-call]

        if result is None:
            return None
        else:
            from math import sqrt
            return sqrt(result)


class Norm1:
    def __init__(self) -> None:
        self.abs_sum = 0.0

    def step(self, value: float) -> None:
        self.abs_sum += abs(value)

    def finalize(self) -> float:
        return self.abs_sum


class Norm2:
    def __init__(self) -> None:
        self.square_sum = 0.0

    def step(self, value: float) -> None:
        self.square_sum += value**2

    def finalize(self) -> float:
        from math import sqrt
        return sqrt(self.square_sum)


def my_sprintf(format: str, arg: str) -> str:
    return format % arg

# }}}


def auto_gather(filenames: List[str]) -> sqlite3.Connection:
    # allow for creating ungathered files.
    # Check if database has been gathered, if not, create one in memory

    # until no files have been checked, assume none have been gathered
    gathered = False
    # check if any of the provided files have been gathered
    for f in filenames:
        db = sqlite3.connect(f)
        cur = db.cursor()

        # get a list of tables with the name of 'runs'
        res = list(cur.execute("""
                            SELECT name
                            FROM sqlite_master
                            WHERE type='table' AND name='runs'
                                          """))
        # there exists a table with the name of 'runs'
        if len(res) == 1:
            gathered = True

    if gathered:
        # gathered files should only have one file
        if len(filenames) > 1:
            raise Exception("Runalyzing multiple gathered files is not supported!!!")

        return sqlite3.connect(filenames[0])

    # create in memory database of files to be gathered
    from logpyle.runalyzer_gather import (FeatureGatherer, gather_multi_file,
                                          make_name_map, scan)
    print("Creating an in memory database from provided files")
    from os.path import exists
    infiles = [f for f in filenames if exists(f)]
    # list of run features as {name: sql_type}
    fg = FeatureGatherer(False, None)
    features, dbname_to_run_id = scan(fg, infiles)

    fmap = make_name_map("")
    qmap = make_name_map("")

    connection = gather_multi_file(":memory:", infiles, fmap, qmap, fg, features,
                             dbname_to_run_id)
    return connection


# {{{ main program

def make_wrapped_db(filenames: List[str], interactive: bool, mangle: bool) -> RunDB:
    db = auto_gather(filenames)
    db.create_aggregate("stddev", 1, StdDeviation)  # type: ignore[arg-type]
    db.create_aggregate("var", 1, Variance)
    db.create_aggregate("norm1", 1, Norm1)  # type: ignore[arg-type]
    db.create_aggregate("norm2", 1, Norm2)  # type: ignore[arg-type]

    db.create_function("sprintf", 2, my_sprintf)
    from math import pow, sqrt
    db.create_function("sqrt", 1, sqrt)
    db.create_function("pow", 2, pow)

    if mangle:
        db_wrap_class: Type[RunDB] = MagicRunDB
    else:
        db_wrap_class = RunDB

    return db_wrap_class(db, interactive=interactive)

# }}}

# vim: foldmethod=marker
