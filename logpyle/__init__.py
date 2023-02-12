"""
Log Quantity Abstract Interfaces
--------------------------------

.. autoclass:: LogQuantity
.. autoclass:: PostLogQuantity
.. autoclass:: MultiLogQuantity
.. autoclass:: MultiPostLogQuantity

Log Manager
-----------

.. autoclass:: LogManager
.. autofunction:: add_run_info

Built-in Log General-Purpose Quantities
---------------------------------------
.. autoclass:: IntervalTimer
.. autoclass:: LogUpdateDuration
.. autoclass:: EventCounter
.. autoclass:: TimestepCounter
.. autoclass:: StepToStepDuration
.. autoclass:: TimestepDuration
.. autoclass:: InitTime
.. autoclass:: CPUTime
.. autoclass:: ETA
.. autofunction:: add_general_quantities

Built-in Log Simulation-Related Quantities
------------------------------------------
.. autoclass:: SimulationTime
.. autoclass:: Timestep
.. autofunction:: set_dt
.. autofunction:: add_simulation_quantities
"""

__copyright__ = "Copyright (C) 2009-2013 Andreas Kloeckner"

__license__ = """
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""


import logpyle.version
__version__ = logpyle.version.VERSION_TEXT


import logging
logger = logging.getLogger(__name__)

from typing import (List, Callable, Union, Tuple, Optional, Dict, Any,
                    TYPE_CHECKING, Iterable)
from pytools.datatable import DataTable

if TYPE_CHECKING:
    import mpi4py


# {{{ timing function

def time() -> float:
    """Return elapsed CPU time, as a float, in seconds."""
    import os
    time_opt = os.environ.get("PYTOOLS_LOG_TIME") or "wall"
    if time_opt == "wall":
        from time import time
        return time()
    elif time_opt == "rusage":
        from resource import getrusage, RUSAGE_SELF
        return getrusage(RUSAGE_SELF).ru_utime
    else:
        raise RuntimeError("invalid timing method '%s'" % time_opt)

# }}}


# {{{ abstract logging interface

class LogQuantity:
    """A source of a loggable scalar that is gathered at the start of each time step.

    Quantity values are gathered in :meth:`LogManager.tick_before`.

    .. automethod:: __init__
    .. automethod:: tick
    .. autoproperty:: default_aggregator
    .. automethod:: __call__
    """

    sort_weight = 0

    def __init__(self, name: str, unit: Optional[str] = None,
                 description: Optional[str] = None) -> None:
        """Create a new quantity.

        Parameters
        ----------
        name
          Quantity name.

        unit
          Quantity unit.

        description
          Quantity description.
        """
        self.name = name
        self.unit = unit
        self.description = description

    @property
    def default_aggregator(self) -> None:
        """Default rank aggregation function."""
        return None

    def tick(self) -> None:
        """Perform updates required at every :class:`LogManager` tick."""
        pass

    def __call__(self) -> Any:
        """Return the current value of the diagnostic represented by this
        :class:`LogQuantity` or None if no value is available.

        This is only called if the invocation interval calls for it.
        """
        raise NotImplementedError


class PostLogQuantity(LogQuantity):
    """A source of a loggable scalar that is gathered after each time step.

    Quantity values are gathered in :meth:`LogManager.tick_after`.

    .. automethod:: __init__
    .. automethod:: tick
    .. autoproperty:: default_aggregator
    .. automethod:: __call__
    .. automethod:: prepare_for_tick
    """
    sort_weight = 0

    def prepare_for_tick(self) -> None:
        """Perform (optional) update at :meth:`LogManager.tick_before`."""
        pass


class MultiLogQuantity:
    """A source of a list of loggable scalars gathered at the start of each time
    step.

    Quantity values are gathered in :meth:`LogManager.tick_before`.

    .. automethod:: __init__
    .. automethod:: tick
    .. autoproperty:: default_aggregators
    .. automethod:: __call__
    """
    sort_weight = 0

    def __init__(self, names: List[str], units: Optional[List[str]] = None,
                 descriptions: Optional[List[str]] = None) -> None:
        """Create a new quantity.

        Parameters
        ----------
        names
          List of quantity names.

        units
          List of quantity units.

        descriptions
          List of quantity descriptions.
        """
        self.names = names

        if units is None:
            units = len(names) * [""]
        self.units = units

        if descriptions is None:
            descriptions = len(names) * [""]
        self.descriptions = descriptions

    @property
    def default_aggregators(self) -> List[None]:
        """List of default aggregators."""
        return [None] * len(self.names)

    def tick(self) -> None:
        """Perform updates required at every :class:`LogManager` tick."""
        pass

    def __call__(self) -> Iterable[Optional[float]]:
        """Return an iterable of the current values of the diagnostic represented
        by this :class:`MultiLogQuantity`.

        This is only called if the invocation interval calls for it.
        """
        raise NotImplementedError


class MultiPostLogQuantity(MultiLogQuantity, PostLogQuantity):
    """A source of a list of loggable scalars gathered after each time step.

    Quantity values are gathered in :meth:`LogManager.tick_after`.

    .. automethod:: __init__
    .. automethod:: tick
    .. autoproperty:: default_aggregators
    .. automethod:: __call__
    .. automethod:: prepare_for_tick
    """
    pass


class DtConsumer:
    def __init__(self, dt) -> None:
        self.dt = dt

    def set_dt(self, dt) -> None:
        self.dt = dt


class TimeTracker(DtConsumer):
    def __init__(self, dt: Optional[float], start: float = 0) -> None:
        DtConsumer.__init__(self, dt)
        self.t = start

    def tick(self) -> None:
        self.t += self.dt


class SimulationLogQuantity(PostLogQuantity, DtConsumer):
    """A source of loggable scalars that needs to know the simulation timestep."""

    def __init__(self, dt: Optional[float], name: str, unit: Optional[str] = None,
                 description: Optional[str] = None) -> None:
        PostLogQuantity.__init__(self, name, unit, description)
        DtConsumer.__init__(self, dt)


class PushLogQuantity(LogQuantity):
    def __init__(self, name: str, unit: Optional[str] = None,
                 description: Optional[str] = None) -> None:
        LogQuantity.__init__(self, name, unit, description)
        self.value = None

    def push_value(self, value) -> None:
        if self.value is not None:
            raise RuntimeError("can't push two values per cycle")
        self.value = value

    def __call__(self) -> Optional[float]:
        v = self.value
        self.value = None
        return v


class CallableLogQuantityAdapter(LogQuantity):
    """Adapt a 0-ary callable as a :class:`LogQuantity`."""
    def __init__(self, callable: Callable, name: str, unit: Optional[str] = None,
                 description: Optional[str] = None) -> None:
        self.callable = callable
        LogQuantity.__init__(self, name, unit, description)

    def __call__(self) -> float:
        return self.callable()

# }}}


# {{{ manager functionality

class _GatherDescriptor:
    def __init__(self, quantity: LogQuantity, interval: int) -> None:
        self.quantity = quantity
        self.interval = interval


class _QuantityData:
    def __init__(self, unit: str, description: str,
                 default_aggregator: Callable) -> None:
        self.unit = unit
        self.description = description
        self.default_aggregator = default_aggregator


def _join_by_first_of_tuple(list_of_iterables):
    loi = [i.__iter__() for i in list_of_iterables]
    if not loi:
        return
    key_vals = [next(iter) for iter in loi]
    keys = [kv[0] for kv in key_vals]
    values = [kv[1] for kv in key_vals]
    target_key = max(keys)

    force_advance = False

    i = 0
    while True:
        while keys[i] < target_key or force_advance:
            try:
                new_key, new_value = next(loi[i])
            except StopIteration:
                return
            assert keys[i] < new_key
            keys[i] = new_key
            values[i] = new_value
            if new_key > target_key:
                target_key = new_key

            force_advance = False

        i += 1
        if i >= len(loi):
            i = 0

        if min(keys) == target_key:
            yield target_key, values[:]
            force_advance = True


def _get_unique_id() -> str:
    try:
        from uuid import uuid1
    except ImportError:
        try:
            import hashlib
            checksum = hashlib.md5()
        except ImportError:
            # for Python << 2.5
            import md5  # type: ignore
            checksum = md5.new()

        from random import Random
        rng = Random()
        rng.seed()
        for _ in range(20):
            checksum.update(str(rng.randrange(1 << 30)).encode("utf-32"))
        return checksum.hexdigest()
    else:
        return uuid1().hex


def _get_unique_suffix():
    from datetime import datetime
    return "-" + datetime.utcnow().strftime("%Y%m%d-%H%M%S")


def _set_up_schema(db_conn):
    # initialize new database
    db_conn.execute("""
      create table quantities (
        name text,
        unit text,
        description text,
        default_aggregator blob)""")
    db_conn.execute("""
      create table constants (
        name text,
        value blob)""")
    db_conn.execute("""
      create table warnings (
        rank integer,
        step integer,
        message text,
        category text,
        filename text,
        lineno integer
        )""")

    schema_version = 2
    return schema_version


from pytools import Record


class LogManager:
    """A distributed-memory-capable diagnostic time-series logging facility.
    It is meant to log data from a computation, with certain log quantities
    available before a cycle, and certain other ones afterwards. A timeline of
    invocations looks as follows::

        tick_before()
        compute...
        tick()
        tick_after()

        tick_before()
        compute...
        tick_after()

        ...

    In a time-dependent simulation, each group of :meth:`tick_before`
    :meth:`tick_after` calls captures data for a single time state,
    namely that in which the data may have been *before* the "compute"
    step. However, some data (such as the length of the timestep taken
    in a time-adpative method) may only be available *after* the completion
    of the "compute..." stage, which is why :meth:`tick_after` exists.

    A :class:`LogManager` logs any number of named time series of floats to
    a file. Non-time-series data, in the form of constants, is also
    supported and saved.

    If MPI parallelism is used, the "head rank" below always refers to
    rank 0.

    Command line tools called :command:`runalyzer` are available for looking
    at the data in a saved log.

    .. automethod:: __init__
    .. automethod:: save
    .. automethod:: close

    .. rubric:: Data retrieval

    .. automethod:: get_table
    .. automethod:: get_warnings
    .. automethod:: get_expr_dataset
    .. automethod:: get_joint_dataset

    .. rubric:: Configuration

    .. automethod:: capture_warnings
    .. automethod:: add_watches
    .. automethod:: set_watch_interval
    .. automethod:: set_constant
    .. automethod:: add_quantity

    .. rubric:: Time Loop

    .. automethod:: tick_before
    .. automethod:: tick_after
    """

    def __init__(self, filename: Optional[str] = None, mode: str = "r",
                 mpi_comm: Optional["mpi4py.MPI.Comm"] = None,
                 capture_warnings: bool = True, commit_interval: float = 90,
                 watch_interval: float = 1.0) -> None:
        """Initialize this log manager instance.

        :param filename: If given, the filename to which this log is bound.
          If this database exists, the current state is loaded from it.
        :param mode: One of "w", "r" for write, read. "w" assumes that the
          database is initially empty. May also be "wu" to indicate that
          a unique filename should be chosen automatically. May also be "wo"
          to indicate that the file should be overwritten.
        :arg mpi_comm: An optional :class:`mpi4py.MPI.Comm` object.
          If given, logs are periodically synchronized to the head node,
          which then writes them out to disk.
        :param capture_warnings: Tap the Python warnings facility and save warnings
          to the log file.
        :param commit_interval: actually perform a commit only every N times a commit
          is requested.
        :param watch_interval: print watches every N seconds.
        """

        assert isinstance(mode, str), "mode must be a string"
        assert mode in ["w", "r", "wu", "wo"], "invalid mode"

        self.quantity_data: Dict[str, _QuantityData] = {}
        self.last_values: Dict[str, Optional[float]] = {}
        self.before_gather_descriptors: List[_GatherDescriptor] = []
        self.after_gather_descriptors: List[_GatherDescriptor] = []
        self.tick_count = 0

        self.commit_interval = commit_interval
        self.commit_countdown = commit_interval

        self.constants: Dict[str, object] = {}

        self.last_save_time = time()

        # self-timing
        self.start_time = time()
        self.t_log: float = 0

        # parallel support
        self.head_rank = 0
        self.mpi_comm = mpi_comm
        self.is_parallel = mpi_comm is not None

        if mpi_comm is None:
            self.rank = 0
        else:
            self.rank = mpi_comm.rank
            self.head_rank = 0

        # watch stuff
        self.watches: List[Record] = []
        self.have_nonlocal_watches = False

        # Interval between printing watches, in seconds
        self.set_watch_interval(watch_interval)

        # database binding
        import sqlite3 as sqlite

        if filename is None:
            file_base = ":memory:"
            file_extension = ""
        else:
            import os
            file_base, file_extension = os.path.splitext(filename)
            if self.is_parallel:
                file_base += "-rank%d" % self.rank

        while True:
            suffix = ""

            if mode == "wu" and not file_base == ":memory:":
                if self.is_parallel:
                    assert self.mpi_comm
                    suffix = self.mpi_comm.bcast(_get_unique_suffix(),
                                                 root=self.head_rank)
                else:
                    suffix = _get_unique_suffix()

            filename = file_base + suffix + file_extension

            if mode == "wo":
                import os
                try:
                    os.remove(filename)
                except OSError:
                    pass

            self.db_conn = sqlite.connect(filename, timeout=30)
            self.mode = mode
            try:
                self.db_conn.execute("select * from quantities;")
            except sqlite.OperationalError:
                # we're building a new database
                if mode == "r":
                    raise RuntimeError("Log database '%s' not found" % filename)

                self.schema_version = _set_up_schema(self.db_conn)
                self.set_constant("schema_version", self.schema_version)

                self.set_constant("is_parallel", self.is_parallel)

                # set globally unique run_id
                if self.is_parallel:
                    assert self.mpi_comm
                    self.set_constant("unique_run_id",
                            self.mpi_comm.bcast(_get_unique_id(),
                                root=self.head_rank))
                else:
                    self.set_constant("unique_run_id", _get_unique_id())

                if self.is_parallel:
                    assert self.mpi_comm
                    self.set_constant("rank_count", self.mpi_comm.Get_size())
                else:
                    self.set_constant("rank_count", 1)

            else:
                # we've opened an existing database
                if mode == "w":
                    raise RuntimeError("Log database '%s' already exists" % filename)

                if mode == "wu":
                    # try again with a new suffix
                    continue

                if mode == "wo":
                    # try again, someone might have created a file with the same name
                    continue

                self._load()

            break

        self.old_showwarning: Optional[Callable] = None
        if capture_warnings:
            self.capture_warnings(True)

    def capture_warnings(self, enable: bool = True) -> None:
        def _showwarning(message, category, filename, lineno, file=None, line=None):
            try:
                self.old_showwarning(message, category, filename, lineno, file, line)
            except TypeError:
                # cater to Python 2.5 and earlier
                self.old_showwarning(message, category, filename, lineno)

            if self.schema_version >= 1 and self.mode[0] == "w":
                if self.schema_version >= 2:
                    self.db_conn.execute("insert into warnings values (?,?,?,?,?,?)",
                            (self.rank, self.tick_count, str(message), str(category),
                                filename, lineno))
                else:
                    self.db_conn.execute("insert into warnings values (?,?,?,?,?)",
                            (self.tick_count, str(message), str(category),
                                filename, lineno))

        import warnings
        if enable:
            if self.old_showwarning is None:
                pass
                self.old_showwarning = warnings.showwarning
                warnings.showwarning = _showwarning
            else:
                raise RuntimeError("Warnings capture was enabled twice")
        else:
            if self.old_showwarning is None:
                raise RuntimeError(
                        "Warnings capture was disabled, but never enabled")

            warnings.showwarning = self.old_showwarning
            self.old_showwarning = None

    def _load(self) -> None:
        if self.mpi_comm and self.mpi_comm.rank != self.head_rank:
            return

        from pickle import loads
        for name, value in self.db_conn.execute("select name, value from constants"):
            self.constants[name] = loads(value)

        self.schema_version = self.constants.get("schema_version", 0)

        self.is_parallel = bool(self.constants["is_parallel"])

        for name, unit, description, def_agg in self.db_conn.execute(
                "select name, unit, description, default_aggregator "
                "from quantities"):
            self.quantity_data[name] = _QuantityData(
                    unit, description, loads(def_agg))

    def close(self) -> None:
        if self.old_showwarning is not None:
            self.capture_warnings(False)

        self.save()
        self.db_conn.close()

    def get_table(self, q_name: str) -> DataTable:
        if q_name not in self.quantity_data:
            raise KeyError("invalid quantity name '%s'" % q_name)

        result = DataTable(["step", "rank", "value"])

        for row in self.db_conn.execute(
                "select step, rank, value from %s" % q_name):
            result.insert_row(row)

        return result

    def get_warnings(self) -> DataTable:
        columns = ["step", "message", "category", "filename", "lineno"]
        if self.schema_version >= 2:
            columns.insert(0, "rank")

        result = DataTable(columns)

        for row in self.db_conn.execute(
                "select %s from warnings" % (", ".join(columns))):
            result.insert_row(row)

        return result

    def add_watches(self, watches: List[Union[str, Tuple[str, str]]]) -> None:
        """Add quantities that are printed after every time step.

        :param watches:
            List of expressions to watch. Each element can either be
            a string of the expression to watch, or a tuple of the expression
            and a format string. In the format string, you can use the custom
            fields ``{display}``, ``{value}``, and ``{unit}`` to indicate where the
            watch expression, value, and unit should be printed. The default format
            string for each watch is ``{display}={value:g}{unit}``.
        """
        from pytools import Record

        class WatchInfo(Record):
            pass

        default_format = "{display}={value:g}{unit} | "

        for watch in watches:
            if isinstance(watch, tuple):
                expr, fmt = watch
            else:
                expr = watch
                fmt = default_format

            parsed = self._parse_expr(expr)
            parsed, dep_data = self._get_expr_dep_data(parsed)

            if len(dep_data) == 1:
                unit = dep_data[0].qdat.unit
            else:
                unit = None

            from pytools import any
            self.have_nonlocal_watches = self.have_nonlocal_watches or \
                    any(dd.nonlocal_agg for dd in dep_data)

            from pymbolic import compile  # type: ignore
            compiled = compile(parsed, [dd.varname for dd in dep_data])

            watch_info = WatchInfo(parsed=parsed, expr=expr, dep_data=dep_data,
                    compiled=compiled, unit=unit, format=fmt)

            self.watches.append(watch_info)

    def set_watch_interval(self, interval: float) -> None:
        """Set the interval (in seconds) between the time watches are printed.

        :param interval: watch printing interval in seconds.
        """
        self.watch_interval = interval
        self.next_watch_tick = self.tick_count + 1

    def set_constant(self, name: str, value: Any) -> None:
        """Make a named, constant value available in the log.

        :param name: the name of the constant.
        :param value: the value of the constant.
        """
        existed = name in self.constants
        self.constants[name] = value

        from pickle import dumps
        value = bytes(dumps(value))

        if existed:
            self.db_conn.execute("update constants set value = ? where name = ?",
                    (value, name))
        else:
            self.db_conn.execute("insert into constants values (?,?)",
                    (name, value))

        self._commit()

    def _insert_datapoint(self, name: str, value: Optional[float]) -> None:
        if value is None:
            return

        self.last_values[name] = value

        try:
            self.db_conn.execute("insert into %s values (?,?,?)" % name,
                    (self.tick_count, self.rank, float(value)))
        except Exception:
            print("while adding datapoint for '%s':" % name)
            raise

    def _update_t_log(self, name: str, value: float) -> None:
        if value is None:
            return

        self.last_values[name] = value

        try:
            self.db_conn.execute(f"update {name} set value = {float(value)} \
                where rank = {self.rank} and step = {self.tick_count}")
        except Exception:
            print("while adding datapoint for '%s':" % name)
            raise

    def _gather_for_descriptor(self, gd) -> None:
        if self.tick_count % gd.interval == 0:
            q_value = gd.quantity()
            if isinstance(gd.quantity, MultiLogQuantity):
                for name, value in zip(gd.quantity.names, q_value):
                    self._insert_datapoint(name, value)
            else:
                self._insert_datapoint(gd.quantity.name, q_value)

    def tick(self) -> None:
        """Record data points from each added :class:`LogQuantity`.

        May also checkpoint data to disk, and/or synchronize data points
        to the head rank.
        """
        from warnings import warn
        warn("LogManager.tick() is deprecated. "
                "Use LogManager.tick_{before,after}().",
                DeprecationWarning)

        self.tick_before()
        self.tick_after()

    def tick_before(self) -> None:
        """Record data points from each added :class:`LogQuantity` that
        is not an instance of :class:`PostLogQuantity`. Also, invoke
        :meth:`PostLogQuantity.prepare_for_tick` on :class:`PostLogQuantity`
        instances.
        """
        tick_start_time = time()

        for gd in self.before_gather_descriptors:
            self._gather_for_descriptor(gd)

        for gd in self.after_gather_descriptors:
            from typing import cast
            cast(PostLogQuantity, gd.quantity).prepare_for_tick()

        self.t_log = time() - tick_start_time

    def tick_after(self) -> None:
        """Record data points from each added :class:`LogQuantity` that
        is an instance of :class:`PostLogQuantity`.

        May also checkpoint data to disk.
        """
        tick_start_time = time()

        for gd_lst in [self.before_gather_descriptors,
                self.after_gather_descriptors]:
            for gd in gd_lst:
                gd.quantity.tick()

        for gd in self.after_gather_descriptors:
            self._gather_for_descriptor(gd)

        if tick_start_time - self.start_time > 15*60:
            save_interval = 5*60
        else:
            save_interval = 15

        if tick_start_time > self.last_save_time + save_interval:
            self.save()

        # print watches
        if self.tick_count+1 >= self.next_watch_tick:
            self._watch_tick()

        self.t_log += time() - tick_start_time

        # Adjust log update time(s), t_log
        for gd in self.after_gather_descriptors:
            if isinstance(gd.quantity, LogUpdateDuration):
                self._update_t_log(gd.quantity.name, gd.quantity())

        self.tick_count += 1

    def _commit(self) -> None:
        self.commit_countdown -= 1
        if self.commit_countdown <= 0:
            self.commit_countdown = self.commit_interval
            self.db_conn.commit()

    def save(self) -> None:
        from sqlite3 import OperationalError
        try:
            self.db_conn.commit()
        except OperationalError as e:
            from warnings import warn
            warn("encountered sqlite error during commit: %s" % e)

        self.last_save_time = time()

    def add_quantity(self, quantity: LogQuantity, interval: int = 1) -> None:
        """Add a :class:`LogQuantity` to this manager.

        :param quantity: add the specified :class:`LogQuantity`.
        :param interval: interval (in time steps) when to gather this quantity.
        """

        def add_internal(name, unit, description, def_agg):
            logger.debug("add log quantity '%s'" % name)

            if name in self.quantity_data:
                raise RuntimeError("cannot add the same quantity '%s' twice" % name)
            self.quantity_data[name] = _QuantityData(unit, description, def_agg)

            from pickle import dumps
            self.db_conn.execute("""insert into quantities values (?,?,?,?)""", (
                name, unit, description,
                bytes(dumps(def_agg))))
            self.db_conn.execute("""create table %s
              (step integer, rank integer, value real)""" % name)

            self._commit()

        gd = _GatherDescriptor(quantity, interval)
        if isinstance(quantity, PostLogQuantity):
            gd_list = self.after_gather_descriptors
        else:
            gd_list = self.before_gather_descriptors

        gd_list.append(gd)
        gd_list.sort(key=lambda gd: gd.quantity.sort_weight)

        if isinstance(quantity, MultiLogQuantity):
            for name, unit, description, def_agg in zip(
                    quantity.names,
                    quantity.units,
                    quantity.descriptions,
                    quantity.default_aggregators):
                add_internal(name, unit, description, def_agg)
        else:
            add_internal(quantity.name,
                    quantity.unit, quantity.description,
                    quantity.default_aggregator)

    def get_expr_dataset(self, expression, description=None, unit=None):
        """Prepare a time-series dataset for a given expression.

        :arg expression: A :mod:`pymbolic` expression that may involve
          the time-series variables and the constants in this :class:`LogManager`.
          If there is data from multiple ranks for a quantity occurring in
          this expression, an aggregator may have to be specified.
        :returns: ``(description, unit, table)``, where *table*
          is a list of tuples ``(tick_nbr, value)``.

        Aggregators are specified as follows:
        - ``qty.min``, ``qty.max``, ``qty.avg``, ``qty.sum``, ``qty.norm2``,
        ``qty.median``
        - ``qty[rank_nbr]``
        - ``qty.loc``
        """

        parsed = self._parse_expr(expression)
        parsed, dep_data = self._get_expr_dep_data(parsed)

        # aggregate table data
        for dd in dep_data:
            table = self.get_table(dd.name)
            table.sort(["step"])
            dd.table = table.aggregated(["step"], "value", dd.agg_func).data

        # evaluate unit and description, if necessary
        if unit is None:
            from pymbolic import substitute, parse

            unit_dict = {dd.varname: dd.qdat.unit for dd in dep_data}
            from pytools import all
            if all(v is not None for v in unit_dict.values()):
                unit_dict = {k: parse(v) for k, v in unit_dict.items()}
                unit = substitute(parsed, unit_dict)
            else:
                unit = None

        if description is None:
            description = expression

        # compile and evaluate
        from pymbolic import compile
        compiled = compile(parsed, [dd.varname for dd in dep_data])

        data = []

        for key, values in _join_by_first_of_tuple(dd.table for dd in dep_data):
            try:
                data.append((key, compiled(*values)))
            except ZeroDivisionError:
                pass

        return (description, unit, data)

    def get_joint_dataset(self, expressions):
        """Return a joint data set for a list of expressions.

        :arg expressions: a list of either strings representing
          expressions directly, or triples (descr, unit, expr).
          In the former case, the description and the unit are
          found automatically, if possible. In the latter case,
          they are used as specified.
        :returns: A triple ``(descriptions, units, table)``, where
            *table* is a a list of ``[(tstep, (val_expr1, val_expr2,...)...]``.
        """

        # dubs is a list of (desc, unit, table) triples as
        # returned by get_expr_dataset
        dubs = []
        for expr in expressions:
            if isinstance(expr, str):
                dub = self.get_expr_dataset(expr)
            else:
                expr_descr, expr_unit, expr_str = expr
                dub = self.get_expr_dataset(
                        expr_str,
                        description=expr_descr,
                        unit=expr_unit)

            dubs.append(dub)

        zipped_dubs = list(zip(*dubs))
        zipped_dubs[2] = list(
                _join_by_first_of_tuple(zipped_dubs[2]))

        return zipped_dubs

    def get_plot_data(self, expr_x, expr_y, min_step=None, max_step=None):
        """Generate plot-ready data.

        :return: ``(data_x, descr_x, unit_x), (data_y, descr_y, unit_y)``
        """
        (descr_x, descr_y), (unit_x, unit_y), data = \
                self.get_joint_dataset([expr_x, expr_y])
        if min_step is not None:
            data = [(step, tup) for step, tup in data if min_step <= step]
        if max_step is not None:
            data = [(step, tup) for step, tup in data if step <= max_step]

        stepless_data = [tup for step, tup in data]

        if stepless_data:
            data_x, data_y = list(zip(*stepless_data))
        else:
            data_x = []
            data_y = []

        return (data_x, descr_x, unit_x), \
               (data_y, descr_y, unit_y)

    def write_datafile(self, filename, expr_x, expr_y) -> None:
        (data_x, label_x), (data_y, label_y) = self.get_plot_data(
                expr_x, expr_y)

        outf = open(filename, "w")
        outf.write(f"# {label_x} vs. {label_y}")
        for dx, dy in zip(data_x, data_y):
            outf.write("{}\t{}\n".format(repr(dx), repr(dy)))
        outf.close()

    def plot_matplotlib(self, expr_x, expr_y) -> None:
        from matplotlib.pyplot import xlabel, ylabel, plot

        (data_x, descr_x, unit_x), (data_y, descr_y, unit_y) = \
                self.get_plot_data(expr_x, expr_y)

        xlabel(f"{descr_x} [{unit_x}]")
        ylabel(f"{descr_y} [{unit_y}]")
        plot(data_x, data_y)

    # {{{ private functionality

    def _parse_expr(self, expr):
        from pymbolic import parse, substitute
        parsed = parse(expr)

        # substitute in global constants
        parsed = substitute(parsed, self.constants)

        return parsed

    def _get_expr_dep_data(self, parsed):
        class Nth:
            def __init__(self, n):
                self.n = n

            def __call__(self, lst):
                return lst[self.n]

        from pymbolic.mapper.dependency import DependencyMapper  # type: ignore

        deps = DependencyMapper(include_calls=False)(parsed)

        # gather information on aggregation expressions
        dep_data = []
        from pymbolic.primitives import Variable, Lookup, Subscript  # type: ignore
        for dep_idx, dep in enumerate(deps):
            nonlocal_agg = True

            if isinstance(dep, Variable):
                name = dep.name

                if name == "math":
                    continue

                agg_func = self.quantity_data[name].default_aggregator
                if agg_func is None:
                    if self.is_parallel:
                        raise ValueError(
                                "must specify explicit aggregator for '%s'" % name)

                    agg_func = lambda lst: lst[0]
            elif isinstance(dep, Lookup):
                assert isinstance(dep.aggregate, Variable)
                name = dep.aggregate.name
                agg_name = dep.name

                if agg_name == "loc":
                    agg_func = Nth(self.rank)
                    nonlocal_agg = False
                elif agg_name == "min":
                    agg_func = min
                elif agg_name == "max":
                    agg_func = max
                elif agg_name == "avg":
                    from statistics import fmean
                    agg_func = fmean
                elif agg_name == "median":
                    from statistics import median
                    agg_func = median
                elif agg_name == "sum":
                    agg_func = sum
                elif agg_name == "norm2":
                    from math import sqrt
                    agg_func = lambda iterable: sqrt(
                            sum(entry**2 for entry in iterable))
                else:
                    raise ValueError("invalid rank aggregator '%s'" % agg_name)
            elif isinstance(dep, Subscript):
                assert isinstance(dep.aggregate, Variable)
                name = dep.aggregate.name

                from pymbolic import evaluate
                agg_func = Nth(evaluate(dep.index))

            qdat = self.quantity_data[name]

            class DependencyData(Record):
                pass

            this_dep_data = DependencyData(name=name, qdat=qdat, agg_func=agg_func,
                    varname="logvar%d" % dep_idx, expr=dep,
                    nonlocal_agg=nonlocal_agg)
            dep_data.append(this_dep_data)

        # substitute in the "logvar" variable names
        from pymbolic import var, substitute
        parsed = substitute(parsed,
                {dd.expr: var(dd.varname) for dd in dep_data})

        return parsed, dep_data

    def _calculate_next_watch_tick(self) -> None:
        ticks_per_interval = (self.tick_count/max(1, time()-self.start_time)
                         * self.watch_interval)
        self.next_watch_tick = self.tick_count + int(max(1, ticks_per_interval))

    def _watch_tick(self) -> None:
        """Print the watches after a tick."""
        if not self.have_nonlocal_watches and self.rank != self.head_rank:
            return

        data_block = {qname: self.last_values.get(qname, 0)
                for qname in self.quantity_data.keys()}

        if self.mpi_comm is not None and self.have_nonlocal_watches:
            gathered_data = self.mpi_comm.gather(data_block, self.head_rank)
        else:
            gathered_data = [data_block]

        if self.rank == self.head_rank:
            assert gathered_data

            values: Dict[str, list] = {}
            for data_block in gathered_data:
                for name, value in data_block.items():
                    values.setdefault(name, []).append(value)

            def compute_watch_str(watch):
                display = watch.expr
                unit = watch.unit if watch.unit not in ["1", None] else ""
                value = watch.compiled(
                        *[dd.agg_func(values[dd.name])
                            for dd in watch.dep_data])
                try:
                    return f"{watch.format}".format(display=display, value=value,
                                                    unit=unit)
                except ZeroDivisionError:
                    return "%s:div0" % watch.display
            if self.watches:
                print("".join(
                        compute_watch_str(watch) for watch in self.watches),
                      flush=True)

        self._calculate_next_watch_tick()

        if self.mpi_comm is not None and self.have_nonlocal_watches:
            self.next_watch_tick = self.mpi_comm.bcast(
                    self.next_watch_tick, self.head_rank)

    # }}}

# }}}


# {{{ actual data loggers

class _SubTimer:
    def __init__(self, itimer) -> None:
        self.itimer = itimer
        self.elapsed = 0

    def start(self):
        self.start_time = time()
        return self

    def stop(self):
        self.elapsed += time() - self.start_time
        del self.start_time
        return self

    def __enter__(self) -> None:
        self.start()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()
        self.submit()

    def submit(self) -> None:
        self.itimer.add_time(self.elapsed)
        self.elapsed = 0


class IntervalTimer(PostLogQuantity):
    """Records elapsed times supplied by the user either through
    sub-timers, or by explicitly calling :meth:`add_time`.

    .. automethod:: __init__
    .. automethod:: get_sub_timer
    .. automethod:: start_sub_timer
    .. automethod:: add_time
    """

    def __init__(self, name: str, description: Optional[str] = None) -> None:
        LogQuantity.__init__(self, name, "s", description)
        self.elapsed: float = 0

    def get_sub_timer(self):
        return _SubTimer(self)

    def start_sub_timer(self):
        sub_timer = _SubTimer(self)
        sub_timer.start()
        return sub_timer

    def add_time(self, t: float) -> None:
        self.start_time = time()
        self.elapsed += t

    def __call__(self) -> float:
        result = self.elapsed
        self.elapsed = 0
        return result


class LogUpdateDuration(PostLogQuantity):
    """Records how long the last log update in :class:`LogManager` took.

    .. automethod:: __init__
    """

    def __init__(self, mgr: LogManager, name: str = "t_log") -> None:
        LogQuantity.__init__(self, name, "s", "Time spent updating the log")
        self.log_manager = mgr

    def __call__(self) -> float:
        return self.log_manager.t_log


class EventCounter(PostLogQuantity):
    """Counts events signaled by :meth:`add`.

    .. automethod:: __init__
    .. automethod:: add
    .. automethod:: transfer
    """

    def __init__(self, name: str = "interval",
                 description: Optional[str] = None) -> None:
        PostLogQuantity.__init__(self, name, "1", description)
        self.events = 0

    def add(self, n: int = 1) -> None:
        self.events += n

    def transfer(self, counter) -> None:
        self.events += counter.pop()

    def prepare_for_tick(self) -> None:
        self.events = 0

    def __call__(self) -> int:
        result = self.events
        return result


def time_and_count_function(f, timer, counter=None, increment=1) -> Callable:
    def inner_f(*args, **kwargs):
        if counter is not None:
            counter.add(increment)
        sub_timer = timer.start_sub_timer()
        try:
            return f(*args, **kwargs)
        finally:
            sub_timer.stop().submit()

    return inner_f


class TimestepCounter(LogQuantity):
    """Counts the number of times :class:`LogManager` ticks."""

    def __init__(self, name: str = "step") -> None:
        LogQuantity.__init__(self, name, "1", "Timesteps")
        self.steps = 0

    def __call__(self) -> int:
        result = self.steps
        self.steps += 1
        return result


class StepToStepDuration(PostLogQuantity):
    """Records the CPU time between invocations of
    :meth:`LogManager.tick_before` and
    :meth:`LogManager.tick_after`.

    .. automethod:: __init__
    """

    def __init__(self, name: str = "t_2step") -> None:
        PostLogQuantity.__init__(self, name, "s", "Step-to-step duration")
        self.last_start_time: Optional[float] = None
        self.last2_start_time: Optional[float] = None

    def prepare_for_tick(self) -> None:
        self.last2_start_time = self.last_start_time
        self.last_start_time = time()

    def __call__(self) -> Optional[float]:
        if self.last2_start_time is None or self.last_start_time is None:
            return None
        else:
            return self.last_start_time - self.last2_start_time


class TimestepDuration(PostLogQuantity):
    """Records the CPU time between the starts of time steps.
    :meth:`LogManager.tick_before` and
    :meth:`LogManager.tick_after`.

    .. automethod:: __init__
    """

    # We would like to run last, so that if log gathering takes any
    # significant time, we catch that, too. (CUDA sync-on-time-taking,
    # I'm looking at you.)
    sort_weight = 1000

    def __init__(self, name: str = "t_step") -> None:
        PostLogQuantity.__init__(self, name, "s", "Time step duration")

    def prepare_for_tick(self) -> None:
        self.last_start = time()

    def __call__(self) -> float:
        now = time()
        result = now - self.last_start
        del self.last_start
        return result


class InitTime(LogQuantity):
    """Stores the time it took for the application to initialize.

    Measures the time from process start to the start of the first time step.

    .. automethod:: __init__
    """

    def __init__(self, name: str = "t_init") -> None:
        LogQuantity.__init__(self, name, "s", "Init time")

        import os
        try:
            import psutil
        except ModuleNotFoundError:
            from warnings import warn
            warn("Measuring the init time requires the 'psutil' module.")
            self.done = True
        else:
            p = psutil.Process(os.getpid())
            self.start_time = p.create_time()
            self.done = False

    def __call__(self) -> Optional[float]:
        if self.done:
            return None

        self.done = True
        now = time()
        return now - self.start_time


class CPUTime(LogQuantity):
    """Records (monotonically increasing) CPU time.

    .. automethod:: __init__
    """
    def __init__(self, name: str = "t_cpu") -> None:
        LogQuantity.__init__(self, name, "s", "Wall time")

        self.start = time()

    def __call__(self) -> float:
        return time()-self.start


class ETA(LogQuantity):
    """Records an estimate of how long the computation will still take.

    .. automethod:: __init__
    """
    def __init__(self, total_steps: int, name: str = "t_eta") -> None:
        LogQuantity.__init__(self, name, "s", "Estimated remaining duration")

        self.steps = 0
        self.total_steps = total_steps
        self.start = time()

    def __call__(self) -> float:
        fraction_done = self.steps/self.total_steps
        self.steps += 1
        time_spent = time()-self.start
        if fraction_done > 1e-9:
            return time_spent/fraction_done-time_spent
        else:
            return 0


def add_general_quantities(mgr: LogManager) -> None:
    """Add generally applicable :class:`LogQuantity` objects to *mgr*."""

    mgr.add_quantity(TimestepDuration())
    mgr.add_quantity(StepToStepDuration())
    mgr.add_quantity(CPUTime())
    mgr.add_quantity(LogUpdateDuration(mgr))
    mgr.add_quantity(TimestepCounter())
    mgr.add_quantity(InitTime())


class SimulationTime(TimeTracker, LogQuantity):
    """Record (monotonically increasing) simulation time."""

    def __init__(self, dt: Optional[float], name: str = "t_sim",
                 start: float = 0) -> None:
        LogQuantity.__init__(self, name, "s", "Simulation Time")
        TimeTracker.__init__(self, dt, start)

    def __call__(self) -> float:
        return self.t


class Timestep(SimulationLogQuantity):
    """Record the magnitude of the simulated time step."""

    def __init__(self, dt: Optional[float], name: str = "dt",
                 unit: str = "s") -> None:
        SimulationLogQuantity.__init__(self, dt, name, unit, "Simulation Timestep")

    def __call__(self) -> float:
        return self.dt


def set_dt(mgr: LogManager, dt: float) -> None:
    """Set the simulation timestep on :class:`LogManager` ``mgr`` to ``dt``."""

    for gd_lst in [mgr.before_gather_descriptors,
            mgr.after_gather_descriptors]:
        for gd in gd_lst:
            if isinstance(gd.quantity, DtConsumer):
                gd.quantity.set_dt(dt)


def add_simulation_quantities(mgr: LogManager, dt: Optional[float] = None) -> None:
    """Add :class:`LogQuantity` objects relating to simulation time.

    :param mgr: the :class:`LogManager` instance.
    :param dt: (deprecated, use :meth:`set_dt` instead)
    """
    if dt is not None:
        from warnings import warn
        warn("Specifying dt ahead of time is a deprecated practice. "
                "Use logpyle.set_dt() instead.")

    mgr.add_quantity(SimulationTime(dt))
    mgr.add_quantity(Timestep(dt))


def add_run_info(mgr: LogManager) -> None:
    """Add generic run metadata, such as command line, host, and time."""

    try:
        import psutil
    except ModuleNotFoundError:
        import sys
        mgr.set_constant("cmdline", " ".join(sys.argv))
    else:
        mgr.set_constant("cmdline", " ".join(psutil.Process().cmdline()))

    from socket import gethostname
    mgr.set_constant("machine", gethostname())
    from time import localtime, strftime, time
    mgr.set_constant("date", strftime("%a, %d %b %Y %H:%M:%S %Z", localtime()))
    mgr.set_constant("unixtime", time())

# }}}

# vim: foldmethod=marker
