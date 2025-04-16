import code
from typing import Any

import pandas as pd
from pytools import Table
from sqlalchemy import create_engine

# Commands:
#  .help        show this help message
#  .q SQL       execute a (potentially mangled) query
#  .runprops    show a list of run properties
#  .quantities  show a list of time-dependent quantities

# Plotting:
#  .plot SQL    plot results of (potentially mangled) query.
#               result sets can be (x,y) or (x,y,descr1,descr2,...),
#               in which case a new plot will be started for each
#               tuple (descr1, descr2, ...)
#  .scatter SQL make scatterplot results of (potentially mangled) query.
#               result sets can have between two and four columns
#               for (x,y,size,color).

# SQL mangling, if requested ("MagicSQL"):
#     select $quantity where pred(feature)

# Custom SQLite aggregates:
#     stddev, var, norm1, norm2

# Available Python symbols:
#     db: the SQLite database
#     mangle_sql(query_str): mangle the SQL query string query_str
#     q(query_str): get db cursor for mangled query_str
#     dbplot(cursor): plot result of cursor
#     dbscatter(cursor): make scatterplot result of cursor
#     dbprint(cursor): print result of cursor
#     split_cursor(cursor): x,y,data gather that .plot uses internally
#     table_from_cursor(cursor)


def table_from_df(df: pd.DataFrame, header: tuple[str, ...] | None = None,
                  skip_index: bool = True) -> Table | None:
    if df is None:
        return None

    tbl = Table()

    if header:
        tbl.add_row(header)
    else:
        tbl.add_row(df.columns)  # type: ignore[arg-type]

    for row in df.itertuples():
        if skip_index:
            tbl.add_row(row[1:])
        else:
            tbl.add_row(row)

    return tbl


def pandalyzer_help() -> None:
    print("""
Commands:
 help()                show this help message.
 runprops(prop=None)   show a list of run properties (constants);
                       with the optional argument, show only that property.
 quantities()          show a list of time-dependent quantities.
 warnings()            show a list of warnings.
 dump()                show an SQL table.

Plotting:
 dbplot()              plot list of quantities.

Available Python symbols:
 db                    the database.
""")


def make_pandalyzer_symbols(db: Any) -> dict[str, object]:
    return {
            "__name__": "__console__",
            "__doc__": None,
            "help": pandalyzer_help,
            "runprops": db.runprops,
            "quantities": db.quantities,
            "warnings": db.warnings,
            "db": db,
            "dump": db.dump,
            "dbplot": db.plot,
            # "q": db.q,
            # "dbplot": db.plot_cursor,
            # "dbscatter": db.scatter_cursor,
            # "dbprint": db.print_cursor,
            # "split_cursor": split_cursor,
            # "table_from_cursor": table_from_cursor,
            }


class RunDB:
    def __init__(self, engine: Any, interactive: bool) -> None:
        self.engine = engine
        self.interactive = interactive
        self.rank_agg_tables: set[Any] = set()
        self.tables: dict[str, pd.DataFrame] = {}

    def _get_table(self, table_name: str) -> pd.DataFrame:
        try:
            return self.tables[table_name]
        except KeyError:
            self.tables[table_name] = pd.read_sql_table(table_name, self.engine)
            return self.tables[table_name]

    def runprops(self, prop: str | None = None) -> None:
        if prop:
            tbl = Table()
            tbl.add_row(("Property", "Value"))
            tbl.add_row((prop, self._get_table("runs")[prop].values[0]))

            print(tbl)
        else:
            print(table_from_df(self._get_table("runs").transpose(),
              header=("Property", "Value"), skip_index=False))

    def quantities(self, where: Any = None) -> None:
        res = self._get_table("quantities")
        print(len(res), "quantities.")
        print(table_from_df(res))

    def plot(self, values: list[str], **kwargs: Any) -> Any:
        from matplotlib.pyplot import legend, show

        if len(values) < 2:
            raise ValueError("Need at least two elements in 'values'.")

        import itertools

        colors = itertools.cycle(["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
              "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
              "#bcbd22", "#17becf"])

        data = []
        legend_entries = []
        for v in values:
            data.append(self._get_table(v).value)
            unit = self.get_unit_for_quantity(v)
            unit = " [" + unit + "]" if unit != "1" else ""

            legend_entries.append(v + unit)

        df = pd.concat(data, axis=1, keys=values)

        p = df.plot(x=values[0], y=values[1], color=next(colors), **kwargs)

        for v in values[2:]:
            p = df.plot(x=values[0], y=v, ax=p, color=next(colors), **kwargs)

        legend(legend_entries[1:])
        p.axes.set_xlabel(legend_entries[0])
        p.axes.set_ylabel("\n".join(legend_entries[1:]))
        show(block=False)
        return p

    def dump(self, table_name: str) -> None:
        print(table_from_df(self._get_table(table_name)))

    def get_unit_for_quantity(self, quantity: str) -> str:
        q = self._get_table("quantities")
        return q.loc[q.name == quantity].unit.to_string(index=False).strip()

    def warnings(self) -> None:
        self.dump("warnings")


def make_db(file: str, interactive: bool) -> RunDB:
    engine = create_engine(f"sqlite:///{file}")
    return RunDB(engine, interactive=interactive)


class PandalyzerConsole(code.InteractiveConsole):
    def __init__(self, db: Any) -> None:
        self.db = db
        code.InteractiveConsole.__init__(self,
                make_pandalyzer_symbols(db))

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

        try:
            import atexit
            import os
            import readline
            import rlcompleter  # noqa: F401

            histfile = os.path.join(os.environ["HOME"], ".runalyzerhist")
            if os.access(histfile, os.R_OK):
                readline.read_history_file(histfile)
            atexit.register(readline.write_history_file, histfile)
            readline.parse_and_bind("tab: complete")
        except ImportError:
            pass

        self.last_push_result = False

    def push(self, cmdline: str) -> bool:
        self.last_push_result = code.InteractiveConsole.push(self, cmdline)

        return self.last_push_result

        # elif cmd == "plot":
        #     cursor = self.db.db.execute(self.db.mangle_sql(args))
        #     columnnames = [column[0] for column in cursor.description]
        #     self.db.plot_cursor(cursor, labels=columnnames)
        # elif cmd == "scatter":
        #     cursor = self.db.db.execute(self.db.mangle_sql(args))
        #     columnnames = [column[0] for column in cursor.description]
        #     self.db.scatter_cursor(cursor, labels=columnnames)
        # else:
        #     print("invalid magic command")
