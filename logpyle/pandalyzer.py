import code
import pandas as pd
from sqlalchemy import create_engine
from warnings import warn
from pytools import Table

try:
    import readline
    import rlcompleter  # noqa: F401
    HAVE_READLINE = True
except ImportError:
    HAVE_READLINE = False


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

def table_from_df(df, header=None, skip_index=True):
    tbl = Table()

    if header:
        tbl.add_row(header)
    else:
        tbl.add_row(df.columns)

    for row in df.itertuples():
        if skip_index:
            tbl.add_row(row[1:])
        else:
            tbl.add_row(row)

    return tbl

def pandalyzer_help():
    print("""
Commands:
 help()       show this help message
 runprops()   show a list of run properties
 quantities() show a list of time-dependent quantities

Plotting:
 plot()       plot list of quantities.

Custom SQLite aggregates:
    stddev, var, norm1, norm2

Available Python symbols:
    db: the database
    q(query_str): get db cursor for mangled query_str
    dbplot(cursor): plot result of cursor
    dbscatter(cursor): make scatterplot result of cursor
    dbprint(cursor): print result of cursor
    split_cursor(cursor): x,y,data gather that .plot uses internally
    table_from_cursor(cursor)
""")

def make_pandalyzer_symbols(db):
    return {
            "__name__": "__console__",
            "__doc__": None,
            "help" : pandalyzer_help,
            "runprops" : db.runprops,
            "quantities" : db.quantities,
            "db": db,
            "dump": db.dump,
            "plot": db.plot,
            ## "q": db.q,
            # "dbplot": db.plot_cursor,
            # "dbscatter": db.scatter_cursor,
            # "dbprint": db.print_cursor,
            # "split_cursor": split_cursor,
            # "table_from_cursor": table_from_cursor,
            }


class RunDB:
    def __init__(self, engine, interactive):
        self.engine = engine
        self.interactive = interactive
        self.rank_agg_tables = set()
        self.tables = {}

    def _get_table(self, table_name: str):
        try:
            return self.tables[table_name]
        except KeyError:
            try:
                self.tables[table_name] = pd.read_sql_table(table_name, self.engine)
                return self.tables[table_name]
            except ValueError:
                if table_name == "runs":
                    warn("Run runalyzer-gather first.")
                else:
                    warn(f"No such table '{table_name}'.")


    def runprops(self):
        print(table_from_df(self._get_table("runs").transpose(), header=["Property", "Value"], skip_index=False))

    def quantities(self) -> None:
        print(table_from_df(self._get_table("quantities")))

    def plot(self, values: list, kind: str = "line"):
        from matplotlib.pyplot import plot, show, legend

        data = []
        legend_entries = []
        for v in values:
            data.append(self._get_table(v).value)
            legend_entries.append(v+ " ["+self.get_unit_for_quantity(v)+"]")


        df = pd.concat(data, axis=1, keys=values)

        # print(data)
        # df = pd.DataFrame([data]).transpose()
        # print(df)
        p = df.plot(kind=kind)
        # print(keys)
        legend(legend_entries)
        # for v in values:
        # p.axes.set_xlabel(values[0])
        p.axes.set_xlabel("step")
        show()
        return p

    def dump(self, table_name: str) -> None:
        print(table_from_df(self._get_table(table_name)))

    def get_unit_for_quantity(self, quantity: str) -> str:
        q = self._get_table("quantities")
        print(type(q.loc[q.name==quantity].unit))
        return q.loc[q.name==quantity].unit.to_string(index=False)



def make_db(file, interactive):
    engine = create_engine(f"sqlite:///{file}")
    return RunDB(engine, interactive=interactive)


class PandalyzerConsole(code.InteractiveConsole):
    def __init__(self, db):
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

        if HAVE_READLINE:
            import os
            import atexit

            histfile = os.path.join(os.environ["HOME"], ".runalyzerhist")
            if os.access(histfile, os.R_OK):
                readline.read_history_file(histfile)
            atexit.register(readline.write_history_file, histfile)
            readline.parse_and_bind("tab: complete")

        self.last_push_result = False

    def push(self, cmdline):
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

