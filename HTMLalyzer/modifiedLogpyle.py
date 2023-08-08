from typing import (TYPE_CHECKING, Any, Callable, Dict, Generator, Iterable,
                    List, Optional, Sequence, TextIO, Tuple, Type, Union, cast)
from time import monotonic as time_monotonic
from dataclasses import dataclass
from pytools.datatable import DataTable

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


@dataclass(frozen=True)
class _GatherDescriptor:
    quantity: LogQuantity
    interval: int


@dataclass(frozen=True)
class _QuantityData:
    unit: Optional[str]
    description: Optional[str]
    default_aggregator: Optional[Callable[..., Any]]



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

    .. rubric:: Time Loop

    .. automethod:: tick_before
    .. automethod:: tick_after
    """

    def __init__(self, filename: Optional[str] = None, mode: str = "r",
                 mpi_comm: Optional["mpi4py.MPI.Comm"] = None,
                 capture_warnings: bool = True, commit_interval: int = 90,
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
        :arg capture_warnings: Tap the Python warnings facility and save warnings
          to the log file.
        :arg commit_interval: actually perform a commit only every N times a commit
          is requested.
        :arg watch_interval: print watches every N seconds.
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

        # watch stuff
        self.watches: List[_WatchInfo] = []
        self.have_nonlocal_watches = False

        # Interval between printing watches, in seconds
        self.set_watch_interval(watch_interval)

        # database binding
        import sqlite3 as sqlite

        self.sqlite_filename: Optional[str] = None
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

        # {{{ warnings/logging capture

        self.warning_data: List[_LogWarningInfo] = []
        self.old_showwarning: Optional[Callable[..., Any]] = None
        if capture_warnings and self.mode[0] == "w":
            self.capture_warnings(True)

        self.logging_data: List[_LogWarningInfo] = []
        self.logging_handler: Optional[logging.Handler] = None
        if capture_logging and self.mode[0] == "w":
            self.capture_logging(True)

        # }}}

    def get_logging(self) -> DataTable:
        # Match the table set up by _set_up_schema
        columns = ["rank", "step", "unixtime", "level", "message", "filename",
                   "lineno"]

        result = DataTable(columns)

        if self.schema_version < 3:
            from warnings import warn
            warn("This database lacks a 'logging' table")
            return result

        for row in self.db_conn.execute(
                "select %s from logging" % (", ".join(columns))):
            result.insert_row(row)

        return result

    def get_warnings(self) -> DataTable:
        # Match the table set up by _set_up_schema
        columns = ["step", "message", "category", "filename", "lineno"]
        if self.schema_version >= 2:
            columns.insert(0, "rank")

            if self.schema_version >= 3:
                columns.insert(2, "unixtime")

        result = DataTable(columns)

        for row in self.db_conn.execute(
                "select %s from warnings" % (", ".join(columns))):
            result.insert_row(row)

        return result

    def capture_warnings(self, enable: bool = True) -> None:
        def _showwarning(message: Union[Warning, str], category: Type[Warning],
                         filename: str, lineno: int, file: Optional[TextIO] = None,
                         line: Optional[str] = None) -> None:
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
                raise RuntimeError("Warnings capture was enabled twice")
        else:
            if self.old_showwarning is None:
                raise RuntimeError(
                        "Warnings capture was disabled, but never enabled")

            warnings.showwarning = self.old_showwarning
            self.old_showwarning = None

    def capture_logging(self, enable: bool = True) -> None:
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
                warn("Logging capture already enabled")
        else:
            if self.logging_handler:
                root_logger.removeHandler(self.logging_handler)
            self.logging_handler = None

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
        if self.old_showwarning is not None:
            self.capture_warnings(False)

        if self.logging_handler:
            self.capture_logging(False)

        self.save()
        self.db_conn.close()

    def add_watches(self, watches: List[Union[str, Tuple[str, str]]]) -> None:
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

            from pymbolic import compile  # type: ignore[import]
            compiled = compile(parsed, [dd.varname for dd in dep_data])

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

        if tick_start_time - self.start_time > 15*60:
            save_interval = 5*60
        else:
            save_interval = 15

        if tick_start_time > self.last_save_time + save_interval:
            self.save()

        # print watches
        if self.tick_count+1 >= self.next_watch_tick:
            self._watch_tick()

        self.t_log += time_monotonic() - tick_start_time

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

    def save_logging(self) -> None:
        for log in self.logging_data:
            self.db_conn.execute(
                "insert into logging values (?,?,?,?,?,?,?)",
                (self.rank, log.tick_count, log.time,
                log.category, log.message, log.filename,
                log.lineno))

        self.logging_data = []

    def save_warnings(self) -> None:
        for w in self.warning_data:
            self.db_conn.execute(
                "insert into warnings values (?,?,?,?,?,?,?)",
                (self.rank, w.tick_count, w.time, w.message,
                    w.category, w.filename, w.lineno))

        self.warning_data = []

    def save(self) -> None:
        if self.mode[0] == "w":
            self.save_logging()
            self.save_warnings()

        from sqlite3 import OperationalError
        try:
            self.db_conn.commit()
        except OperationalError as e:
            from warnings import warn
            warn("encountered sqlite error during commit: %s" % e)

        self.last_save_time = time_monotonic()

    def add_quantity(self, quantity: LogQuantity, interval: int = 1) -> None:
        """Add a :class:`LogQuantity` to this manager.

        :arg quantity: add the specified :class:`LogQuantity`.
        :arg interval: interval (in time steps) when to gather this quantity.
        """

        def add_internal(name: str, unit: Optional[str], description: Optional[str],
                         def_agg: Optional[Callable[..., Any]]) -> None:
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
