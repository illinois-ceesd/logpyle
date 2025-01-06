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
.. autoclass:: WallTime
.. autoclass:: ETA
.. autoclass:: MemoryHwm
.. autoclass:: GCStats
.. autofunction:: add_general_quantities

Built-in Log Simulation-Related Quantities
------------------------------------------
.. autoclass:: SimulationTime
.. autoclass:: Timestep
.. autofunction:: set_dt
.. autofunction:: add_simulation_quantities


Internal stuff that is only here because the documentation tool wants it
------------------------------------------------------------------------
.. autoclass:: _SubTimer
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

import importlib.metadata

__version__ = importlib.metadata.version(__package__ or __name__)

import logging
import sys
from collections.abc import Callable, Generator, Iterable, Sequence
from dataclasses import dataclass
from sqlite3 import Connection
from time import monotonic as time_monotonic
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    TextIO,
    cast,
)

from pymbolic.compiler import CompiledExpression
from pymbolic.primitives import ExpressionNode
from pytools.datatable import DataTable

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import mpi4py


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

    def __init__(self, name: str, unit: str | None = None,
                 description: str | None = None) -> None:
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

    def __init__(self, names: list[str],
                 units: Sequence[str | None] | None = None,
                 descriptions: Sequence[str | None] | None = None) -> None:
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
            self.units: Sequence[str | None] = len(names) * [None]
        else:
            self.units = units

        if descriptions is None:
            self.descriptions: Sequence[str | None] = len(names) * [None]
        else:
            self.descriptions = descriptions

    @property
    def default_aggregators(self) -> list[None]:
        """List of default aggregators."""
        return [None] * len(self.names)

    def tick(self) -> None:
        """Perform updates required at every :class:`LogManager` tick."""
        pass

    def __call__(self) -> Iterable[float | None]:
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
    def __init__(self) -> None:
        self.dt: float | None = None

    def set_dt(self, dt: float | None) -> None:
        self.dt = dt


class TimeTracker(DtConsumer):
    def __init__(self, start: float = 0) -> None:
        DtConsumer.__init__(self)
        self.t = start

    def tick(self) -> None:
        self.t += cast(float, self.dt)


class SimulationLogQuantity(PostLogQuantity, DtConsumer):
    """A source of loggable scalars that needs to know the simulation timestep."""

    def __init__(self, name: str, unit: str | None = None,
                 description: str | None = None) -> None:
        PostLogQuantity.__init__(self, name, unit, description)
        DtConsumer.__init__(self)


class PushLogQuantity(LogQuantity):
    def __init__(self, name: str, unit: str | None = None,
                 description: str | None = None) -> None:
        LogQuantity.__init__(self, name, unit, description)
        self.value: float | None = None

    def push_value(self, value: float) -> None:
        if self.value is not None:
            raise RuntimeError("can't push two values per cycle")
        self.value = value

    def __call__(self) -> float | None:
        v = self.value
        self.value = None
        return v


class CallableLogQuantityAdapter(LogQuantity):
    """Adapt a 0-ary callable as a :class:`LogQuantity`."""
    def __init__(self, callable: Callable[[], float], name: str,
                 unit: str | None = None, description: str | None = None) \
                    -> None:
        self.callable = callable
        LogQuantity.__init__(self, name, unit, description)

    def __call__(self) -> float:
        return self.callable()

# }}}


# {{{ manager functionality

@dataclass(frozen=True)
class _GatherDescriptor:
    quantity: LogQuantity
    interval: int


@dataclass(frozen=True)
class _QuantityData:
    unit: str | None
    description: str | None
    default_aggregator: Callable[..., Any] | None


def _join_by_first_of_tuple(list_of_iterables: list[Iterable[Any]]) \
        -> Generator[tuple[int, list[Any]], None, None]:
    loi = [i.__iter__() for i in list_of_iterables]
    if not loi:
        return

    # every iterator must have >= 1 object
    try:
        key_vals = [next(iter) for iter in loi]
    except StopIteration:
        return

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
    from uuid import uuid1
    return uuid1().hex


def _get_unique_suffix() -> str:
    from datetime import datetime
    return "-" + datetime.utcnow().strftime("%Y%m%d-%H%M%S")


def _set_up_schema(db_conn: Connection) -> int:
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

    # schema_version < 2 is missing the 'rank' field.
    # schema_version < 3 is missing the 'unixtime' field.
    db_conn.execute("""
      create table warnings (
        rank integer,
        step integer,
        unixtime integer,
        message text,
        category text,
        filename text,
        lineno integer
        )""")

    # schema_version < 3 does not have the logging table
    db_conn.execute("""
      create table logging (
        rank integer,
        step integer,
        unixtime integer,
        level text,
        message text,
        filename text,
        lineno integer
        )""")

    schema_version = 3
    return schema_version


@dataclass
class _DependencyData:
    name: str
    qdat: _QuantityData
    agg_func: Callable[..., Any]
    varname: str
    expr: ExpressionNode
    nonlocal_agg: bool
    table: DataTable | None = None


@dataclass
class _WatchInfo:
    parsed: ExpressionNode
    expr: str
    dep_data: list[_DependencyData]
    compiled: CompiledExpression
    unit: str | None
    format: str


@dataclass(frozen=True)
class _LogWarningInfo:
    tick_count: int
    time: float
    message: str
    category: str
    filename: str
    lineno: int


class LogManager:
    """A distributed-memory-capable diagnostic time-series logging facility.
    It is meant to log data from a computation, with certain log quantities
    available before a cycle, and certain other ones afterwards. A timeline of
    invocations looks as follows::

        tick_before()
        compute...
        tick_after()

        tick_before()
        compute...
        tick_after()

        ...

    In a time-dependent simulation, each group of :meth:`tick_before`
    :meth:`tick_after` calls captures data for a single time state,
    namely that in which the data may have been *before* the "compute"
    step. However, some data (such as the length of the timestep taken
    in a time-adaptive method) may only be available *after* the completion
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
    .. automethod:: get_logging
    .. automethod:: get_expr_dataset
    .. automethod:: get_joint_dataset

    .. rubric:: Configuration

    .. automethod:: capture_warnings
    .. automethod:: capture_logging
    .. automethod:: add_watches
    .. automethod:: set_watch_interval
    .. automethod:: set_constant
    .. automethod:: add_quantity
    .. automethod:: enable_save_on_sigterm

    .. rubric:: Time Loop

    .. automethod:: tick_before
    .. automethod:: tick_after
    """

    def __init__(self, filename: str | None = None, mode: str = "r",  # noqa: C901
                 mpi_comm: Optional["mpi4py.MPI.Comm"] = None,
                 capture_warnings: bool = True,
                 watch_interval: float = 1.0,
                 capture_logging: bool = True) -> None:
        """Initialize this log manager instance.

        :arg filename: If given, the filename to which this log is bound.
          If this database exists, the current state is loaded from it.
        :arg mode: One of "w", "r" for write, read. "w" assumes that the
          database is initially empty. May also be "wu" to indicate that
          a unique filename should be chosen automatically. May also be "wo"
          to indicate that the file should be overwritten.
        :arg mpi_comm: An optional :class:`mpi4py.MPI.Comm` object.
          If given, logs are periodically synchronized to the head node,
          which then writes them out to disk.
        :arg capture_warnings: Tap the Python :mod:`warnings` facility and save
          warnings to the log file. Note that when multiple :class:`LogManager`
          instances have warnings capture enabled, the warnings will be saved
          to all instances.
        :arg watch_interval: print watches every N seconds.
        :arg capture_logging: Tap the Python :mod:`logging` facility and save
          logging messages to the log file. Note that when multiple
          :class:`LogManager` instances have logging capture enabled, the
          logging messages will be saved to all instances.
        """

        assert isinstance(mode, str), "mode must be a string"
        assert mode in ["w", "r", "wu", "wo"], "invalid mode"

        self.quantity_data: dict[str, _QuantityData] = {}
        self.last_values: dict[str, float | None] = {}
        self.before_gather_descriptors: list[_GatherDescriptor] = []
        self.after_gather_descriptors: list[_GatherDescriptor] = []
        self.tick_count = 0

        self.constants: dict[str, object] = {}

        self.last_save_time = time_monotonic()

        # self-timing
        self.start_time = time_monotonic()
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

        # weakref finalization
        self.weakref_finalize: Callable[..., Any] = lambda: None

        # watch stuff
        self.watches: list[_WatchInfo] = []
        self.have_nonlocal_watches = False

        # Interval between printing watches, in seconds
        self.set_watch_interval(watch_interval)

        # database binding
        import sqlite3 as sqlite

        self.sqlite_filename: str | None = None
        if filename is None:
            file_base = ":memory:"
            file_extension = ""
        else:
            import os
            file_base, file_extension = os.path.splitext(filename)
            if self.is_parallel:
                file_base += f"-rank{self.rank}"

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
            if not file_base == ":memory:":
                self.sqlite_filename = filename

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
            except sqlite.OperationalError as err:
                # we're building a new database
                if mode == "r":
                    raise RuntimeError(f"Log database '{filename}' not found") from err

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
                    raise RuntimeError(f"Log database '{filename}' already exists")

                if mode == "wu":
                    # try again with a new suffix
                    continue

                if mode == "wo":
                    # try again, someone might have created a file with the same name
                    continue

                self._load()

            break

        # {{{ warnings/logging capture

        self.warning_data: list[_LogWarningInfo] = []
        self.old_showwarning: Callable[..., Any] | None = None
        if capture_warnings and self.mode[0] == "w":
            self.capture_warnings(True)

        self.logging_data: list[_LogWarningInfo] = []
        self.logging_handler: logging.Handler | None = None
        if capture_logging and self.mode[0] == "w":
            self.capture_logging(True)

        # }}}

        # {{{ atexit handling

        import weakref

        # Make sure the database gets saved at exit.
        # Note that this does not handle all possible exit modes:
        # - SIGINT (i.e., Ctrl-C): automatically handled
        # - SIGKILL (i.e., kill -9), os._exit(), Python fatal internal error:
        #   impossible to capture
        # - SIGTERM (i.e., kill): Users must handle the signal explicitly
        #   (e.g. via 'logmgr.enable_save_on_sigterm()')
        self.weakref_finalize = weakref.finalize(self, self.save)

        # FIXME: The weakref keeps the log manager alive until close() is
        # called or the application exits.

        # }}}

    def __del__(self) -> None:
        self.weakref_finalize()

    def enable_save_on_sigterm(self) -> Callable[..., Any] | int | None:
        """Enable saving the log on SIGTERM.

        :returns: The previous SIGTERM handler.
        """
        # See
        # https://mail.python.org/pipermail/python-ideas/2016-February/038471.html
        # on why this only captures SIGTERM.
        import signal

        def sighndl(_signo: int, _stackframe: Any) -> None:
            self.weakref_finalize()
            sys.exit(_signo)

        return signal.signal(signal.SIGTERM, sighndl)

    def capture_warnings(self, enable: bool = True) -> None:
        """Enable or disable :mod:`warnings` capture."""
        def _showwarning(message: Warning | str, category: type[Warning],
                         filename: str, lineno: int, file: TextIO | None = None,
                         line: str | None = None) -> None:
            assert self.old_showwarning
            self.old_showwarning(message, category, filename, lineno, file, line)

            from time import time

            self.warning_data.append(_LogWarningInfo(
                tick_count=self.tick_count,
                time=time(),
                message=str(message),
                category=str(category),
                filename=filename,
                lineno=lineno
            ))

        import warnings
        if enable:
            if self.schema_version < 3:
                raise ValueError("Warnings capture needs at least schema_version 3, "
                                f" got {self.schema_version}")
            if self.old_showwarning is None:
                self.old_showwarning = warnings.showwarning
                warnings.showwarning = _showwarning
            else:
                from warnings import warn
                warn("Warnings capture already enabled", stacklevel=2)
        else:
            if self.old_showwarning is None:
                from warnings import warn
                warn("Warnings capture already disabled", stacklevel=2)
            else:
                warnings.showwarning = self.old_showwarning
                self.old_showwarning = None

    def capture_logging(self, enable: bool = True) -> None:
        """Enable or disable :mod:`logging` capture."""
        class LogpyleLogHandler(logging.Handler):
            def __init__(self, mgr: LogManager) -> None:
                logging.Handler.__init__(self)
                self.mgr = mgr

            def emit(self, record: logging.LogRecord) -> None:
                from time import time
                self.mgr.logging_data.append(
                    _LogWarningInfo(tick_count=self.mgr.tick_count,
                                time=time(),
                                message=record.getMessage(),
                                category=record.levelname,
                                filename=record.pathname,
                                lineno=record.lineno))

        root_logger = logging.getLogger()

        if enable:
            if self.schema_version < 3:
                raise ValueError("Logging capture needs at least schema_version 3, "
                                f" got {self.schema_version}")
            if self.mode[0] == "w" and self.logging_handler is None:
                self.logging_handler = LogpyleLogHandler(self)
                root_logger.addHandler(self.logging_handler)
            elif self.logging_handler:
                from warnings import warn
                warn("Logging capture already enabled", stacklevel=2)
        else:
            if self.logging_handler:
                root_logger.removeHandler(self.logging_handler)
            elif self.logging_handler is None:
                from warnings import warn
                warn("Logging capture already disabled", stacklevel=2)

            self.logging_handler = None

    def get_logging(self) -> DataTable:
        """Return a :class:`~pytools.datatable.DataTable` of :mod:`logging`
        messages logged by this :class:`LogManager` instance."""
        # Match the table set up by _set_up_schema
        columns = ["rank", "step", "unixtime", "level", "message", "filename",
                   "lineno"]

        result = DataTable(columns)

        if self.schema_version < 3:
            from warnings import warn
            warn("This database lacks a 'logging' table", stacklevel=2)
            return result

        for row in self.db_conn.execute(
                "select {} from logging".format(", ".join(columns))):
            result.insert_row(row)

        return result

    def _load(self) -> None:
        if self.mpi_comm and self.mpi_comm.rank != self.head_rank:
            return

        from pickle import loads
        for name, value in self.db_conn.execute("select name, value from constants"):
            self.constants[name] = loads(value)

        self.schema_version = cast(int, self.constants.get("schema_version", 0))

        self.is_parallel = bool(self.constants["is_parallel"])

        for name, unit, description, def_agg in self.db_conn.execute(
                "select name, unit, description, default_aggregator "
                "from quantities"):
            self.quantity_data[name] = _QuantityData(
                    unit, description, loads(def_agg))

    def close(self) -> None:
        """Close this :class:`LogManager` instance."""
        if self.old_showwarning is not None:
            self.capture_warnings(False)

        if self.logging_handler:
            self.capture_logging(False)

        self.weakref_finalize()

        self.save()
        self.db_conn.close()

    def get_table(self, q_name: str) -> DataTable:
        """Return a :class:`~pytools.datatable.DataTable` of the data logged
        for the quantity *q_name*."""
        if q_name not in self.quantity_data:
            raise KeyError(f"invalid quantity name '{q_name}'")

        result = DataTable(
            ["step", "rank", "value"])

        for row in self.db_conn.execute(
                f"select step, rank, value from {q_name}"):
            result.insert_row(row)

        return result

    def get_warnings(self) -> DataTable:
        """Return a :class:`~pytools.datatable.DataTable` of warnings logged by
        this :class:`LogManager` instance."""
        # Match the table set up by _set_up_schema
        columns = ["step", "message", "category", "filename", "lineno"]
        if self.schema_version >= 2:
            columns.insert(0, "rank")

            if self.schema_version >= 3:
                columns.insert(2, "unixtime")

        result = DataTable(columns)

        for row in self.db_conn.execute(
                "select {} from warnings".format(", ".join(columns))):
            result.insert_row(row)

        return result

    def add_watches(self, watches: list[str | tuple[str, str]]) -> None:
        """Add quantities that are printed after every time step.

        :arg watches:
            List of expressions to watch. Each element can either be
            a string of the expression to watch, or a tuple of the expression
            and a format string. In the format string, you can use the custom
            fields ``{display}``, ``{value}``, and ``{unit}`` to indicate where the
            watch expression, value, and unit should be printed. The default format
            string for each watch is ``{display}={value:g}{unit}``.
        """

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

            from pymbolic import compile
            compiled = compile(parsed, [dd.varname for dd in dep_data])  # type: ignore[no-untyped-call]

            watch_info = _WatchInfo(parsed=parsed, expr=expr, dep_data=dep_data,
                                    compiled=compiled, unit=unit, format=fmt)

            self.watches.append(watch_info)

    def set_watch_interval(self, interval: float) -> None:
        """Set the interval (in seconds) between the time watches are printed.

        :arg interval: watch printing interval in seconds.
        """
        self.watch_interval = interval
        self.next_watch_tick = self.tick_count + 1

    def set_constant(self, name: str, value: Any) -> None:
        """Make a named, constant value available in the log.

        :arg name: the name of the constant.
        :arg value: the value of the constant.
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

    def _insert_datapoint(self, name: str, value: float | None) -> None:
        if value is None:
            return

        self.last_values[name] = value

        try:
            self.db_conn.execute(f"insert into {name} values (?,?,?)",
                    (self.tick_count, self.rank, float(value)))
        except Exception:
            print(f"while adding datapoint for '{name}':")
            raise

    def _update_t_log(self, name: str, value: float) -> None:
        if value is None:
            return

        self.last_values[name] = value

        try:
            self.db_conn.execute(f"update {name} set value = {float(value)} \
                where rank = {self.rank} and step = {self.tick_count}")
        except Exception:
            print(f"while adding datapoint for '{name}':")
            raise

    def _gather_for_descriptor(self, gd: _GatherDescriptor) -> None:
        if self.tick_count % gd.interval == 0:
            q_value = gd.quantity()
            if isinstance(gd.quantity, MultiLogQuantity):
                for name, value in zip(gd.quantity.names, q_value, strict=False):
                    self._insert_datapoint(name, value)
            else:
                self._insert_datapoint(gd.quantity.name, q_value)

    def tick_before(self) -> None:
        """Record data points from each added :class:`LogQuantity` that
        is not an instance of :class:`PostLogQuantity`. Also, invoke
        :meth:`PostLogQuantity.prepare_for_tick` on :class:`PostLogQuantity`
        instances.
        """
        tick_start_time = time_monotonic()

        for gd in self.before_gather_descriptors:
            self._gather_for_descriptor(gd)

        for gd in self.after_gather_descriptors:
            cast(PostLogQuantity, gd.quantity).prepare_for_tick()

        # For the first three ticks, force saving the log.
        if self.tick_count < 3:
            self.save()

        self.t_log = time_monotonic() - tick_start_time

    def tick_after(self) -> None:
        """Record data points from each added :class:`LogQuantity` that
        is an instance of :class:`PostLogQuantity`.

        May also checkpoint data to disk.
        """
        tick_start_time = time_monotonic()

        for gd_lst in [self.before_gather_descriptors,
                self.after_gather_descriptors]:
            for gd in gd_lst:
                gd.quantity.tick()

        for gd in self.after_gather_descriptors:
            self._gather_for_descriptor(gd)

        save_interval_seconds = 10

        if tick_start_time > self.last_save_time + save_interval_seconds:
            self.save()

        # print watches
        if self.tick_count + 1 >= self.next_watch_tick:
            self._watch_tick()

        self.t_log += time_monotonic() - tick_start_time

        # Adjust log update time(s), t_log
        for gd in self.after_gather_descriptors:
            if isinstance(gd.quantity, LogUpdateDuration):
                self._update_t_log(gd.quantity.name, gd.quantity())

        self.tick_count += 1

    def _save_logging(self) -> None:
        for log in self.logging_data:
            self.db_conn.execute(
                "insert into logging values (?,?,?,?,?,?,?)",
                (self.rank, log.tick_count, log.time,
                log.category, log.message, log.filename,
                log.lineno))

        self.logging_data = []

    def _save_warnings(self) -> None:
        for w in self.warning_data:
            self.db_conn.execute(
                "insert into warnings values (?,?,?,?,?,?,?)",
                (self.rank, w.tick_count, w.time, w.message,
                    w.category, w.filename, w.lineno))

        self.warning_data = []

    def save(self) -> None:
        """Commit the current state of the log."""
        if self.mode[0] != "w":
            # No need to save readonly files.
            return

        self._save_logging()
        self._save_warnings()

        from sqlite3 import OperationalError
        try:
            self.db_conn.commit()
        except OperationalError as e:
            # Even when encountering a commit error, we want to continue
            # running the application.
            from warnings import warn
            warn(f"encountered sqlite error during commit: {e}", stacklevel=2)

        self.last_save_time = time_monotonic()

    def add_quantity(self, quantity: LogQuantity, interval: int = 1) -> None:
        """Add a :class:`LogQuantity` to this manager.

        :arg quantity: add the specified :class:`LogQuantity`.
        :arg interval: interval (in time steps) when to gather this quantity.
        """

        def add_internal(name: str, unit: str | None, description: str | None,
                         def_agg: Callable[..., Any] | None) -> None:
            logger.debug(f"adding log quantity '{name}'")

            if name in self.quantity_data:
                raise RuntimeError(f"cannot add the same quantity '{name}' twice")
            self.quantity_data[name] = _QuantityData(unit, description, def_agg)

            from pickle import dumps
            self.db_conn.execute("""insert into quantities values (?,?,?,?)""", (
                name, unit, description,
                bytes(dumps(def_agg))))
            self.db_conn.execute(f"""create table {name}
              (step integer, rank integer, value real)""")

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
                    quantity.default_aggregators, strict=False):
                add_internal(name, unit, description, def_agg)
        else:
            add_internal(quantity.name,
                    quantity.unit, quantity.description,
                    quantity.default_aggregator)

        self.save()

    def get_expr_dataset(self, expression: str,
                         description: str | None = None,
                         unit: str | None = None) \
                            -> tuple[str | Any, str | Any,
                                     list[tuple[int, Any]]]:
        """Prepare a time-series dataset for a given expression.

        :arg expression: A :mod:`pymbolic`-like expression that may involve
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
            dd.table = table.aggregated(["step"],  # type: ignore
                                        "value", dd.agg_func).data

        # evaluate unit and description, if necessary
        if unit is None:
            from pymbolic import parse, substitute

            unit_dict: dict[str, Any] = {dd.varname: dd.qdat.unit for dd in dep_data}
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
        compiled = compile(parsed, [dd.varname for dd in dep_data])  # type: ignore[no-untyped-call]

        data = []

        for key, values in _join_by_first_of_tuple(
                [dd.table for dd in dep_data if dd.table]):
            try:
                data.append((key, compiled(*values)))
            except ZeroDivisionError:
                pass

        return (description, unit, data)

    def get_joint_dataset(self, expressions: Sequence[str | tuple[str, str, str]]) \
            -> list[Any]:
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

        zipped_dubs = list(zip(*dubs, strict=False))
        zipped_dubs[2] = list(
                _join_by_first_of_tuple(zipped_dubs[2]))

        return zipped_dubs

    def get_plot_data(self, expr_x: str, expr_y: str,
                      min_step: int | None = None,
                      max_step: int | None = None) \
                            -> tuple[tuple[Any, str, str], tuple[Any, str, str]]:
        """Generate plot-ready data.

        :returns: ``(data_x, descr_x, unit_x), (data_y, descr_y, unit_y)``
        """
        (descr_x, descr_y), (unit_x, unit_y), data = \
                self.get_joint_dataset([expr_x, expr_y])
        if min_step is not None:
            data = [(step, tup) for step, tup in data if min_step <= step]
        if max_step is not None:
            data = [(step, tup) for step, tup in data if step <= max_step]

        stepless_data = [tup for _step, tup in data]

        if stepless_data:
            data_x, data_y = list(zip(*stepless_data, strict=False))
        else:
            data_x = ()
            data_y = ()

        return (data_x, descr_x, unit_x), \
               (data_y, descr_y, unit_y)

    def write_datafile(self, filename: str, expr_x: str,
                       expr_y: str) -> None:
        (data_x, label_x, _), (data_y, label_y, _) = self.get_plot_data(
                expr_x, expr_y)

        outf = open(filename, "w")
        outf.write(f"# {label_x} vs. {label_y}\n")
        for dx, dy in zip(data_x, data_y, strict=False):
            outf.write(f"{dx!r}\t{dy!r}\n")
        outf.close()

    def plot_matplotlib(self, expr_x: str, expr_y: str) -> None:
        from matplotlib.pyplot import plot, xlabel, ylabel

        (data_x, descr_x, unit_x), (data_y, descr_y, unit_y) = \
                self.get_plot_data(expr_x, expr_y)

        xlabel(f"{descr_x} [{unit_x}]")
        ylabel(f"{descr_y} [{unit_y}]")
        plot(data_x, data_y)

    # {{{ private functionality

    def _parse_expr(self, expr: str) -> Any:
        from pymbolic import parse, substitute
        parsed = parse(expr)

        # substitute in global constants
        parsed = substitute(parsed, self.constants)

        return parsed

    def _get_expr_dep_data(self,  # noqa: C901
                           parsed: ExpressionNode) \
            -> tuple[ExpressionNode, list[_DependencyData]]:
        class Nth:
            def __init__(self, n: int) -> None:
                self.n = n

            def __call__(self, lst: list[Any]) -> Any:
                return lst[self.n]

        import pymbolic.mapper.dependency as pmd
        deps = pmd.DependencyMapper(include_calls=False)(parsed)

        # gather information on aggregation expressions
        dep_data = []
        from pymbolic.primitives import Lookup, Subscript, Variable
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
                                f"must specify explicit aggregator for '{name}'")

                    def agg_func(lst: Sequence[Any]) -> Any:
                        return lst[0]
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
                    try:
                        from statistics import fmean
                        agg_func = fmean
                    except ImportError:
                        # fmean is Python 3.8+ only
                        from statistics import mean
                        agg_func = mean
                elif agg_name == "median":
                    from statistics import median
                    agg_func = median
                elif agg_name == "sum":
                    agg_func = sum
                elif agg_name == "norm2":
                    from math import sqrt

                    def agg_func(iterable: Iterable[Any]) -> float:
                        return sqrt(sum(entry ** 2 for entry in iterable))
                else:
                    raise ValueError(f"invalid rank aggregator '{agg_name}'")
            elif isinstance(dep, Subscript):
                assert isinstance(dep.aggregate, Variable)
                name = dep.aggregate.name

                from pymbolic import evaluate
                agg_func = Nth(evaluate(dep.index))

            qdat = self.quantity_data[name]

            assert agg_func

            this_dep_data = _DependencyData(name=name, qdat=qdat, agg_func=agg_func,
                    varname=f"logvar{dep_idx}", expr=dep,
                    nonlocal_agg=nonlocal_agg)
            dep_data.append(this_dep_data)

        # substitute in the "logvar" variable names
        from pymbolic import substitute, var
        parsed = substitute(parsed,
                {dd.expr: var(dd.varname) for dd in dep_data})

        return parsed, dep_data

    def _calculate_next_watch_tick(self) -> None:
        ticks_per_interval = (self.tick_count
                              / max(1, time_monotonic() - self.start_time)
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

            values: dict[str, list[float | None]] = {}
            for data_block in gathered_data:
                for name, value in data_block.items():
                    values.setdefault(name, []).append(value)

            def compute_watch_str(watch: _WatchInfo) -> str:
                display = watch.expr
                unit = watch.unit if watch.unit not in ["1", None] else ""
                value = watch.compiled(
                        *[dd.agg_func(values[dd.name])
                            for dd in watch.dep_data])
                try:
                    return f"{watch.format}".format(display=display, value=value,
                                                    unit=unit)
                except ZeroDivisionError:
                    return f"{display}:div0"
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
    def __init__(self, itimer: "IntervalTimer") -> None:
        self.itimer = itimer
        self.elapsed = 0.0

    def start(self) -> "_SubTimer":
        self.start_time = time_monotonic()
        return self

    def stop(self) -> "_SubTimer":
        self.elapsed += time_monotonic() - self.start_time
        del self.start_time
        return self

    def __enter__(self) -> None:
        self.start()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
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

    def __init__(self, name: str, description: str | None = None) -> None:
        LogQuantity.__init__(self, name, "s", description)
        self.elapsed: float = 0

    def get_sub_timer(self) -> _SubTimer:
        return _SubTimer(self)

    def start_sub_timer(self) -> _SubTimer:
        sub_timer = _SubTimer(self)
        sub_timer.start()
        return sub_timer

    def add_time(self, t: float) -> None:
        self.start_time = time_monotonic()
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
    .. automethod:: pop
    """

    def __init__(self, name: str = "interval",
                 description: str | None = None) -> None:
        PostLogQuantity.__init__(self, name, "1", description)
        self.events = 0

    def add(self, n: int = 1) -> None:
        self.events += n

    def transfer(self, counter: "EventCounter") -> None:
        self.events += counter.pop()

    def pop(self) -> int:
        events = self.events
        self.events = 0
        return events

    def prepare_for_tick(self) -> None:
        self.events = 0

    def __call__(self) -> int:
        result = self.events
        return result


def time_and_count_function(f: Callable[..., Any], timer: IntervalTimer,
                            counter: EventCounter | None = None,
                            increment: int = 1) -> Callable[..., Any]:
    def inner_f(*args: Any, **kwargs: Any) -> Any:
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
    """Records the wall time between the starts of consecutive time steps, i.e.,
    the wall time between :meth:`LogManager.tick_before` of step x and
    :meth:`LogManager.tick_before` of step x+1. The value stored is the value for
    step x+1.

    .. note::

        In most cases, this quantity should approximately match ``t_step`` +
        ``t_log``. If it does not, it might indicate that the application
        performs operations outside :meth:`LogManager.tick_before` and
        :meth:`LogManager.tick_after`, or that some other time is not being
        accounted for.

    .. automethod:: __init__
    """

    def __init__(self, name: str = "t_2step") -> None:
        PostLogQuantity.__init__(self, name, "s", "Step-to-step duration")
        self.last_start_time: float | None = None
        self.last2_start_time: float | None = None

    def prepare_for_tick(self) -> None:
        self.last2_start_time = self.last_start_time
        self.last_start_time = time_monotonic()

    def __call__(self) -> float | None:
        if self.last2_start_time is None or self.last_start_time is None:
            return None
        else:
            return self.last_start_time - self.last2_start_time


class TimestepDuration(PostLogQuantity):
    """Records the wall time between invocations of :meth:`LogManager.tick_before`
    and :meth:`LogManager.tick_after`, i.e., the duration of the time step.

    .. automethod:: __init__
    """

    # We would like to run last, so that if log gathering takes any
    # significant time, we catch that, too. (CUDA sync-on-time-taking,
    # I'm looking at you.)
    sort_weight = 1000

    def __init__(self, name: str = "t_step") -> None:
        PostLogQuantity.__init__(self, name, "s", "Time step duration")

    def prepare_for_tick(self) -> None:
        self.last_start = time_monotonic()

    def __call__(self) -> float:
        now = time_monotonic()
        assert hasattr(self, "last_start"), "tick_after called without tick_before"
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

        try:
            import psutil
        except ModuleNotFoundError:
            from warnings import warn
            warn("Measuring the init time requires the 'psutil' module.", stacklevel=2)
            self.done = True
        else:
            self.create_time = psutil.Process().create_time()
            self.done = False

    def __call__(self) -> float | None:
        if self.done:
            return None

        self.done = True
        from time import time

        # Can't use time_monotonic() here since that does *not* return
        # the time since the UNIX epoch (like time() and
        # psutil.Process.create_time() do), but from another (undefined)
        # reference point.
        return time() - self.create_time


class WallTime(LogQuantity):
    """Records (monotonically increasing) wall time since the quantity was
    initialized.

    .. automethod:: __init__
    """
    def __init__(self, name: str = "t_wall") -> None:
        LogQuantity.__init__(self, name, "s", "Wall time")

        self.start = time_monotonic()

    def __call__(self) -> float:
        return time_monotonic() - self.start


class ETA(LogQuantity):
    """Records an estimate of how long the computation will still take.

    .. automethod:: __init__
    """
    def __init__(self, total_steps: int, name: str = "t_eta") -> None:
        LogQuantity.__init__(self, name, "s", "Estimated remaining duration")

        self.steps = 0
        self.total_steps = total_steps
        self.start = time_monotonic()

    def __call__(self) -> float:
        fraction_done = self.steps / self.total_steps
        self.steps += 1
        time_spent = time_monotonic() - self.start
        if fraction_done > 1e-9:
            return time_spent / fraction_done - time_spent
        else:
            return 0


def add_general_quantities(mgr: LogManager) -> None:
    """Add generally applicable :class:`LogQuantity` objects to *mgr*."""

    mgr.add_quantity(TimestepDuration())
    mgr.add_quantity(StepToStepDuration())
    mgr.add_quantity(WallTime())
    mgr.add_quantity(LogUpdateDuration(mgr))
    mgr.add_quantity(TimestepCounter())
    mgr.add_quantity(InitTime())
    mgr.add_quantity(MemoryHwm())


class SimulationTime(TimeTracker, LogQuantity):
    """Record (monotonically increasing) simulation time."""

    def __init__(self, name: str = "t_sim", start: float = 0) -> None:
        LogQuantity.__init__(self, name, "s", "Simulation Time")
        TimeTracker.__init__(self, start)

    def __call__(self) -> float:
        return self.t


class Timestep(SimulationLogQuantity):
    """Record the magnitude of the simulated time step."""

    def __init__(self, name: str = "dt", unit: str = "s") -> None:
        SimulationLogQuantity.__init__(self, name, unit, "Simulation Timestep")

    def __call__(self) -> float | None:
        return self.dt


def set_dt(mgr: LogManager, dt: float) -> None:
    """Set the simulation timestep on :class:`LogManager` ``mgr`` to ``dt``.

    :arg mgr: the :class:`LogManager` instance.
    :arg dt: the simulation timestep.
    """

    for gd_lst in [mgr.before_gather_descriptors,
            mgr.after_gather_descriptors]:
        for gd in gd_lst:
            if isinstance(gd.quantity, DtConsumer):
                gd.quantity.set_dt(dt)


def add_simulation_quantities(mgr: LogManager) -> None:
    """Add :class:`LogQuantity` objects relating to simulation time.

    :arg mgr: the :class:`LogManager` instance.
    """
    mgr.add_quantity(SimulationTime())
    mgr.add_quantity(Timestep())


def _get_env_vars() -> str:
    """Return a string containing all environment variables."""
    from os import environ
    return "\n".join(f"{key}={value}" for key, value in environ.items())


def add_run_info(mgr: LogManager) -> None:
    """Add generic run metadata, such as command line, host, and time."""

    try:
        import psutil
    except ModuleNotFoundError:
        mgr.set_constant("cmdline", " ".join(sys.argv))
    else:
        mgr.set_constant("cmdline", " ".join(psutil.Process().cmdline()))

    from socket import gethostname
    mgr.set_constant("machine", gethostname())
    from time import localtime, strftime, time
    mgr.set_constant("date", strftime("%a, %d %b %Y %H:%M:%S %Z", localtime()))
    mgr.set_constant("unixtime", time())
    mgr.set_constant("env", _get_env_vars())


class MemoryHwm(PostLogQuantity):
    """Record (monotonically increasing) memory high water mark (HWM) in MBytes."""
    def __init__(self, name: str = "memory_usage_hwm") -> None:
        PostLogQuantity.__init__(self, name, "MByte", "Memory High Water Mark")
        import os
        if os.uname().sysname == "Linux":
            self.fac = 1024
        elif os.uname().sysname == "Darwin":
            self.fac = 1024 * 1024
        else:
            raise ValueError("MemoryHwm is only supported on Linux/Mac.")

    def __call__(self) -> float:
        from resource import RUSAGE_SELF, getrusage
        res = getrusage(RUSAGE_SELF)
        return res.ru_maxrss / self.fac


class GCStats(MultiPostLogQuantity):
    """Record Garbage Collection statistics.

    Information regarding the meaning of these values can be found at:
        - https://docs.python.org/3/library/gc.html
        - https://alex.dzyoba.com/blog/arc-vs-gc

          ..  # noqa: E501
        - https://stackoverflow.com/questions/64561488/pythons-gc-get-objects-from-get-count
        - https://github.com/python/cpython/blob/main/Modules/gcmodule.c
    """
    def __init__(self) -> None:
        names = [  # gc.isenabled():
                  "gc_isenabled",
                   # gc.get_count():
                  "gc_count_gen0", "gc_count_gen1", "gc_count_gen2",
                   # gc.get_stats():
                  "gc_collections_gen0", "gc_collected_gen0",
                  "gc_uncollectable_gen0",
                  "gc_collections_gen1", "gc_collected_gen1",
                  "gc_uncollectable_gen1",
                  "gc_collections_gen2", "gc_collected_gen2",
                  "gc_uncollectable_gen2",
                 ]

        units = ["bool",
                 "1", "1", "1",
                 "1", "1", "1", "1", "1", "1", "1", "1", "1"]

        descriptions = ["Is automatic GC enabled?",
                        "GC count gen0", "GC count gen1", "GC count gen2",
                        "GC collections gen0", "GC objects collected gen0",
                        "GC objects uncollectable gen0",
                        "GC collections gen1", "GC objects collected gen1",
                        "GC objects uncollectable gen1",
                        "GC collections gen2", "GC objects collected gen2",
                        "GC objects uncollectable gen2",
                        ]

        assert len(names) == len(units) == len(descriptions) == 13

        super().__init__(names, cast(list[str | None], units),
                         cast(list[str | None], descriptions))

    def __call__(self) -> Iterable[float | None]:
        import gc

        enabled = gc.isenabled()
        counts = gc.get_count()
        stats = gc.get_stats()

        return [enabled,
                counts[0], counts[1], counts[2],
                stats[0]["collections"], stats[0]["collected"],
                stats[0]["uncollectable"],
                stats[1]["collections"], stats[1]["collected"],
                stats[1]["uncollectable"],
                stats[2]["collections"], stats[2]["collected"],
                stats[2]["uncollectable"]
                ]

# }}}

# vim: foldmethod=marker
