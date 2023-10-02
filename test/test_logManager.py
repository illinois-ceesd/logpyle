import logging
from time import monotonic as time_monotonic
from time import sleep
from warnings import warn

import pytest
from pymbolic.primitives import Variable

from logpyle import (EventCounter, IntervalTimer, LogManager, LogQuantity,
                     PushLogQuantity, add_general_quantities, add_run_info,
                     add_simulation_quantities, set_dt,
                     time_and_count_function)


def test_start_time_has_past(basic_logmgr: LogManager):
    assert basic_logmgr.start_time <= time_monotonic()


def test_empty_on_init(basic_logmgr: LogManager):
    # ensure that there are no initial watches
    assert len(basic_logmgr.watches) == 0


def test_basic_warning():
    with pytest.warns(UserWarning):
        warn("Oof. Something went awry.", UserWarning)


def test_logging_warnings_from_warnings_module(basic_logmgr: LogManager):
    first_warning_message = "Not a warning: First warning message!!!"
    first_warning_type = UserWarning

    basic_logmgr.tick_before()
    warn(first_warning_message, first_warning_type)
    basic_logmgr.tick_after()

    # ensure that the warning was caught properly
    print(basic_logmgr.warning_data[0])
    assert basic_logmgr.warning_data[0].message == first_warning_message
    assert basic_logmgr.warning_data[0].category == str(first_warning_type)
    assert basic_logmgr.warning_data[0].tick_count == 0

    second_warning_message = "Not a warning: Second warning message!!!"
    second_warning_type = UserWarning

    basic_logmgr.tick_before()
    warn(second_warning_message, second_warning_type)
    basic_logmgr.tick_after()

    # ensure that the warning was caught properly
    print(basic_logmgr.warning_data[1])
    assert basic_logmgr.warning_data[1].message == second_warning_message
    assert basic_logmgr.warning_data[1].category == str(second_warning_type)
    assert basic_logmgr.warning_data[1].tick_count == 1

    # save warnings to database
    basic_logmgr.save_warnings()

    # ensure that warnings are of the correct form
    message_ind = basic_logmgr.get_warnings().column_names.index("message")
    step_ind = basic_logmgr.get_warnings().column_names.index("step")
    data = basic_logmgr.get_warnings().data

    # ensure the first warning has been saved correctly
    assert data[0][message_ind] == first_warning_message
    assert data[0][step_ind] == 0

    # ensure the second warning has been saved correctly
    assert data[1][message_ind] == second_warning_message
    assert data[1][step_ind] == 1


def test_logging_warnings_from_logging_module(basic_logmgr: LogManager):
    logger = logging.getLogger(__name__)

    first_warning_message = "Not a warning: First warning message!!!"

    basic_logmgr.tick_before()
    logger.warning(first_warning_message)
    basic_logmgr.tick_after()

    # ensure that the warning was caught properly
    print(basic_logmgr.logging_data)
    assert basic_logmgr.logging_data[0].message == first_warning_message
    assert basic_logmgr.logging_data[0].category == "WARNING"
    assert basic_logmgr.logging_data[0].tick_count == 0

    second_warning_message = "Not a warning: Second warning message!!!"

    basic_logmgr.tick_before()
    logger.warning(second_warning_message)
    basic_logmgr.tick_after()

    # ensure that the warning was caught properly
    print(basic_logmgr.logging_data[1])
    assert basic_logmgr.logging_data[1].message == second_warning_message
    assert basic_logmgr.logging_data[1].category == "WARNING"
    assert basic_logmgr.logging_data[1].tick_count == 1

    # save warnings to database
    basic_logmgr.save_logging()

    # ensure that warnings are of the correct form
    message_ind = basic_logmgr.get_logging().column_names.index("message")
    step_ind = basic_logmgr.get_logging().column_names.index("step")
    data = basic_logmgr.get_logging().data

    # ensure the first warning has been saved correctly
    assert data[0][message_ind] == first_warning_message
    assert data[0][step_ind] == 0

    # ensure the second warning has been saved correctly
    assert data[1][message_ind] == second_warning_message
    assert data[1][step_ind] == 1


def test_general_quantities(basic_logmgr: LogManager):
    # verify that exactly all general quantities were added

    add_general_quantities(basic_logmgr)
    basic_logmgr.tick_before()
    basic_logmgr.tick_after()
    basic_logmgr.save()

    ideal_quantities_added = [
        "t_step",
        "t_wall",
        "t_2step",
        "t_log",
        "memory_usage_hwm",
        "step",
        "t_init",
    ]

    actual_quantities_added = basic_logmgr.db_conn.execute(
        "select * from quantities"
    ).fetchall()

    # reformat into list of quantities
    actual_quantities_added = [desc[0] for desc in actual_quantities_added]

    # sort lists for comparison
    ideal_quantities_added.sort()
    actual_quantities_added.sort()

    assert ideal_quantities_added == actual_quantities_added


def test_simulation_quantities(basic_logmgr: LogManager):
    add_simulation_quantities(basic_logmgr)

    # must set a dt for simulation quantities
    set_dt(basic_logmgr, 0.05)

    basic_logmgr.tick_before()
    sleep(0.01)
    basic_logmgr.tick_after()
    basic_logmgr.save()

    ideal_quantities_added = [
        "t_sim",
        "dt",
    ]

    actual_quantities_added = basic_logmgr.db_conn.execute(
        "select * from quantities"
    ).fetchall()

    # reformat into list of quantities
    actual_quantities_added = [desc[0] for desc in actual_quantities_added]

    # sort lists for comparison
    ideal_quantities_added.sort()
    actual_quantities_added.sort()

    assert ideal_quantities_added == actual_quantities_added


def test_nonexisting_table(basic_logmgr: LogManager):
    add_general_quantities(basic_logmgr)

    with pytest.raises(KeyError):
        basic_logmgr.get_table("nonexistent table")


def test_existing_database_no_overwrite():
    import os

    filename = "THIS_LOG_SHOULD_BE_DELETED.sqlite"
    logmgr = LogManager(filename, "wo")
    add_general_quantities(logmgr)
    logmgr.tick_before()
    logmgr.tick_after()

    logmgr.save()
    logmgr.close()

    assert logmgr.sqlite_filename == filename

    with pytest.raises(RuntimeError):
        logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED.sqlite", "w")

    os.remove(filename)


def test_existing_database_with_overwrite():
    import os

    filename = "THIS_LOG_SHOULD_BE_DELETED.sqlite"
    logmgr = LogManager(filename, "wo")
    add_general_quantities(logmgr)
    print(logmgr.get_expr_dataset("t_wall"))
    logmgr.tick_before()
    logmgr.tick_after()
    print(logmgr.get_expr_dataset("t_wall"))

    logmgr.save()
    logmgr.close()

    assert logmgr.sqlite_filename == filename

    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED.sqlite", "wo")
    # expect the data to have been overwritten
    with pytest.raises(KeyError):
        print(logmgr.get_expr_dataset("t_wall"))

    os.remove(filename)


def test_existing_file_with_overwrite():
    # the os should remove this file before creating the new db
    import os

    filename = "THIS_LOG_SHOULD_BE_DELETED.sqlite"
    with open(filename, "w", encoding="utf-8") as f:
        f.write("This file is a test\n")

    logmgr = LogManager(filename, "wo")
    logmgr.close()

    assert logmgr.sqlite_filename == filename

    os.remove(filename)


def test_open_existing_database():
    import os

    filename = "THIS_LOG_SHOULD_BE_DELETED.sqlite"
    logmgr = LogManager(filename, "wo")

    add_general_quantities(logmgr)

    first_tick = logmgr.get_expr_dataset("t_wall")
    print(first_tick)

    logmgr.tick_before()
    logmgr.tick_after()

    second_tick = logmgr.get_expr_dataset("t_wall")
    print(second_tick)

    logmgr.save()
    logmgr.close()

    assert logmgr.sqlite_filename == filename

    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED.sqlite", "r")
    # expect the data to be the same as the second tick
    saved_data = logmgr.get_expr_dataset("t_wall")
    print(saved_data)
    assert saved_data == second_tick

    assert logmgr.sqlite_filename == filename

    os.remove(filename)


# assuming that a nameless (in memory) database should not save
# data after closing.
def test_in_memory_logmanager():
    # Tests an in memory database
    logmgr = LogManager(None, "wo")
    add_general_quantities(logmgr)
    logmgr.tick_before()
    logmgr.tick_after()

    logmgr.save()
    logmgr.close()

    with pytest.raises(KeyError):
        logmgr = LogManager(None, "wo")
        val = logmgr.get_expr_dataset("t_wall")
        print(val)


def test_reading_in_memory_logmanager():
    # ensure in memory db can not be read
    with pytest.raises(RuntimeError):
        logmgr = LogManager(None, "r")

    # attempt to save in memory db

    logmgr = LogManager(None, "wo")
    add_general_quantities(logmgr)
    logmgr.tick_before()
    logmgr.tick_after()

    logmgr.save()
    logmgr.close()

    # attempt to read saved db, ensure this fails as it shouldnt be saved
    with pytest.raises(RuntimeError):
        logmgr = LogManager(None, "r")
        val = logmgr.get_expr_dataset("t_wall")
        print(val)


test_writing_modes = [
    ("w"),
    ("wo"),
    ("wu"),
]


@pytest.mark.parametrize("mode", test_writing_modes)
def test_in_memory_writing(mode):
    logmgr = LogManager(None, mode)
    add_general_quantities(logmgr)
    logmgr.tick_before()
    logmgr.tick_after()

    logmgr.save()
    logmgr.close()
    print(logmgr.sqlite_filename)
    assert logmgr.sqlite_filename is None


def test_unique_suffix():
    # testing two in a row with no computation in between should force
    # a collision due to the names being based on time of day
    import os

    logmgr1 = LogManager("THIS_LOG_SHOULD_BE_DELETED.sqlite", "wu")
    logmgr1.close()

    logmgr2 = LogManager("THIS_LOG_SHOULD_BE_DELETED.sqlite", "wu")
    logmgr2.close()

    # assert that two distinct databases were created
    db_name1 = logmgr1.sqlite_filename
    db_name2 = logmgr2.sqlite_filename

    # type narrowing the optional type
    assert db_name1
    assert db_name2

    files = os.listdir()
    print(files)
    assert files.count(db_name1) == 1
    assert files.count(db_name2) == 1
    assert db_name1 != db_name2

    os.remove(db_name1)
    os.remove(db_name2)


def test_read_nonexistant_database():
    import os

    fake_file_name = "THIS_LOG_SHOULD_BE_DELETED_AND_DOES_NOT_EXIST"
    with pytest.raises(RuntimeError):
        LogManager(fake_file_name, "r")

    os.remove(fake_file_name)


def test_add_run_info(basic_logmgr: LogManager):
    from socket import gethostname

    time_tol = 0.5

    add_run_info(basic_logmgr)

    saved_machine = basic_logmgr.constants["machine"]
    print(saved_machine)
    assert saved_machine == gethostname()

    from time import time

    # ensure that it is the same day that this log was created
    saved_data = basic_logmgr.constants["date"]
    print(saved_data)

    saved_time = basic_logmgr.constants["unixtime"]
    print(saved_time)
    assert abs(time() - saved_time) < time_tol


def test_set_dt(basic_logmgr: LogManager):
    # Should verify that the dt is set/changed and is applied
    # to dt consuming quantities after changing
    add_simulation_quantities(basic_logmgr)
    for descriptor_list in [
        basic_logmgr.before_gather_descriptors,
        basic_logmgr.after_gather_descriptors,
    ]:
        for descriptor in descriptor_list:
            q_dt = descriptor.quantity.dt
            print(q_dt)
            assert q_dt is None

    set_dt(basic_logmgr, 0.5)

    for descriptor_list in [
        basic_logmgr.before_gather_descriptors,
        basic_logmgr.after_gather_descriptors,
    ]:
        for descriptor in descriptor_list:
            q_dt = descriptor.quantity.dt
            print(q_dt)
            assert q_dt is not None
            assert q_dt == 0.5

    set_dt(basic_logmgr, 0.02)

    for descriptor_list in [
        basic_logmgr.before_gather_descriptors,
        basic_logmgr.after_gather_descriptors,
    ]:
        for descriptor in descriptor_list:
            q_dt = descriptor.quantity.dt
            print(q_dt)
            assert q_dt is not None
            assert q_dt == 0.02


def test_double_enable_warnings(basic_logmgr: LogManager):
    # default is enabled
    with pytest.warns(UserWarning):
        basic_logmgr.capture_warnings(True)


def test_double_disable_warnings(basic_logmgr: LogManager):
    # default is enabled
    basic_logmgr.capture_warnings(False)
    with pytest.warns(UserWarning):
        basic_logmgr.capture_warnings(False)


def test_double_enable_logging(basic_logmgr: LogManager):
    # default is enabled
    with pytest.warns(UserWarning):
        basic_logmgr.capture_logging(True)


def test_double_disable_logging(basic_logmgr: LogManager):
    # default is enabled
    basic_logmgr.capture_logging(False)
    with pytest.warns(UserWarning):
        basic_logmgr.capture_logging(False)


def test_double_add_quantity(basic_logmgr: LogManager):
    class Fifteen(LogQuantity):
        def __call__(self) -> int:
            return 15

    basic_logmgr.add_quantity(Fifteen("fifteen"))
    with pytest.raises(RuntimeError):
        basic_logmgr.add_quantity(Fifteen("fifteen"))


def test_add_watches(basic_logmgr: LogManager):
    # test adding a few watches

    class Fifteen(LogQuantity):
        def __call__(self) -> int:
            return 15

    class FifteenStr(LogQuantity):
        def __call__(self) -> str:
            return "15.0"

    basic_logmgr.add_quantity(Fifteen("name1"))
    basic_logmgr.add_quantity(Fifteen("name2"))
    basic_logmgr.add_quantity(FifteenStr("tup_name1"))

    watch_list = ["name1", ("tup_name1", "str"), "name2"]

    basic_logmgr.add_watches(watch_list)

    basic_logmgr.tick_before()
    # do something ...
    basic_logmgr.tick_before()
    basic_logmgr.save()

    # check that all watches are present
    actual_watches = [watch.expr for watch in basic_logmgr.watches]
    expected = ["name1", "tup_name1", "name2"]
    actual_watches.sort()
    expected.sort()
    print(actual_watches, expected)
    assert actual_watches == expected


def test_time_and_count_function(basic_logmgr: LogManager):
    tol = 0.1

    def func_to_be_timed(t: float):
        sleep(t)
        return

    timer = IntervalTimer("t_duration")
    counter = EventCounter("num_itr")

    basic_logmgr.add_quantity(counter)

    basic_logmgr.tick_before()

    n = 10
    for i in range(n):
        time_and_count_function(func_to_be_timed, timer, counter)(0.1)

    basic_logmgr.tick_after()

    print(timer.elapsed, counter.events)

    assert abs(timer.elapsed - n * 0.1) < tol
    assert counter.events == n


def test_joint_dataset(basic_logmgr: LogManager):
    add_general_quantities(basic_logmgr)
    basic_logmgr.tick_before()
    basic_logmgr.tick_after()
    basic_logmgr.save()

    ideal_quantities_added = [
        "t_step",
        ("cpu time", Variable("s"), "t_wall"),
        "memory_usage_hwm",
    ]
    quantity_names = [
        "t_step",
        "cpu time",
        "memory_usage_hwm",
    ]
    dataset = basic_logmgr.get_joint_dataset(ideal_quantities_added)

    print(dataset)
    names = list(dataset[0])
    names.sort()
    quantity_names.sort()

    for quantity in quantity_names:
        assert quantity in names
    assert len(names) == len(quantity_names)


def test_plot_data(basic_logmgr: LogManager):
    add_general_quantities(basic_logmgr)

    basic_logmgr.tick_before()
    # do something ...
    basic_logmgr.tick_after()

    # there should be one step
    data1 = basic_logmgr.get_plot_data("t_wall", "t_wall")
    print(data1)
    assert len(data1[0][0]) == 1

    basic_logmgr.tick_before()
    # do something ...
    basic_logmgr.tick_after()

    # there should be two steps
    data2 = basic_logmgr.get_plot_data("t_wall", "t_wall")
    print(data2)
    assert len(data2[0][0]) == 2

    basic_logmgr.tick_before()
    # do something ...
    basic_logmgr.tick_after()

    # there should be three steps
    data3 = basic_logmgr.get_plot_data("t_wall", "t_wall")
    print(data3)
    assert len(data3[0][0]) == 3

    # first two of three steps should be taken
    data0_1 = basic_logmgr.get_plot_data("t_wall", "t_wall", 0, 1)
    print(data0_1)
    assert len(data0_1) == 2

    # last two of three steps should be taken
    data1_2 = basic_logmgr.get_plot_data("t_wall", "t_wall", 1, 2)
    print(data1_2)
    assert len(data1_2) == 2


def test_write_datafile(basic_logmgr: LogManager):
    import os

    def has_contents(str1):
        trimmed_str = str1.strip()
        if len(trimmed_str) == 0:
            return False
        else:
            return True

    add_general_quantities(basic_logmgr)

    n = 20

    for i in range(n):
        basic_logmgr.tick_before()
        # do something ...
        basic_logmgr.tick_after()

    filename = "THIS_LOG_SHOULD_BE_DELETED.txt"

    basic_logmgr.write_datafile(filename, "t_wall", "t_wall")

    file_object = open(filename, "r")
    lines = file_object.readlines()
    lines = filter(has_contents, lines)

    i = 0
    for line in lines:
        print(line)
        i += 1

    print(i)
    assert i == n + 1  # n data points plus the title

    os.remove(filename)


def test_plot_matplotlib(basic_logmgr: LogManager):
    pytest.importorskip("matplotlib")

    add_general_quantities(basic_logmgr)

    n = 20

    for i in range(n):
        basic_logmgr.tick_before()
        # do something ...
        basic_logmgr.tick_after()

    basic_logmgr.plot_matplotlib("t_wall", "t_wall")


test_aggregator_data = [
    ("loc", [51, 15, 67, -3, 0, -5, 99, 2], 2),
    ("min", [51, 15, 67, -3, 0, -5, 99, 2], 2),
    ("max", [51, 15, 67, -3, 0, -5, 99, 2], 2),
    ("avg", [51, 15, 67, -3, 0, -5, 99, 2], 2),
    ("median", [51, 15, 67, -3, 0, -5, 99, 2], 2),
    ("sum", [51, 15, 67, -3, 0, -5, 99, 2], 2),
    ("norm2", [51, 15, 67, -3, 0, -5, 99, 2], 2),
    ("NOT_REAL_AGG", [51, 15, 67, -3, 0, -5, 99, 2], 2),
]


@pytest.mark.parametrize("agg, data, expected", test_aggregator_data)
def test_single_rank_aggregator(basic_logmgr, agg, data, expected):
    add_general_quantities(basic_logmgr)

    push_q = PushLogQuantity("value")
    basic_logmgr.add_quantity(push_q)

    for val in data:
        print(val)
        push_q.push_value(val)
        basic_logmgr.tick_before()
        # do something ...
        basic_logmgr.tick_after()

    basic_logmgr.save()

    # NOT_REAL_AGG should raise an error
    if agg == "NOT_REAL_AGG":
        with pytest.raises(ValueError):
            result = basic_logmgr.get_expr_dataset("value." + agg)
        return

    # nothing else should raise an error
    result = basic_logmgr.get_expr_dataset("value." + agg)
    print(result)
    assert result[-1][-1][-1] == expected


def test_eventcounter(basic_logmgr: LogManager):
    # the event counter should keep track of events that occur
    # during the timestep

    counter1 = EventCounter("num_events1")
    counter2 = EventCounter("num_events2")

    basic_logmgr.add_quantity(counter1)

    n = 21
    basic_logmgr.tick_before()

    for i in range(n):
        counter1.add()

    print(counter1.events)
    assert counter1.events == n

    basic_logmgr.tick_after()

    # transfer counter1's count to counter2's
    basic_logmgr.tick_before()

    # at the beggining of tick, counter should clear
    print(counter1.events)
    assert counter1.events == 0

    for i in range(n):
        if i % 3 == 0:
            counter1.add()

    counter2.transfer(counter1)

    assert counter1.events == 0
    assert counter2.events == n / 3

    for i in range(n):
        if i % 3 == 0:
            counter2.add()

    print(counter2.events)
    assert counter2.events == 2 * n / 3

    basic_logmgr.tick_after()


# TODO currently crashes when no timesteps are present
def test_empty_plot_data(basic_logmgr: LogManager):
    add_general_quantities(basic_logmgr)

    # there should be zero step
    data0 = basic_logmgr.get_plot_data("t_wall", "t_wall")
    print(data0)
    assert len(data0[0][0]) == 0

    basic_logmgr.tick_before()
    # do something ...
    basic_logmgr.tick_after()

    # there should be one step
    data1 = basic_logmgr.get_plot_data("t_wall", "t_wall")
    print(data1)
    assert len(data1[0][0]) == 1

    basic_logmgr.tick_before()
    # do something ...
    basic_logmgr.tick_after()

    # there should be two steps
    data2 = basic_logmgr.get_plot_data("t_wall", "t_wall")
    print(data2)
    assert len(data2[0][0]) == 2

    basic_logmgr.tick_before()
    # do something ...
    basic_logmgr.tick_after()

    # there should be three steps
    data3 = basic_logmgr.get_plot_data("t_wall", "t_wall")
    print(data3)
    assert len(data3[0][0]) == 3

    # first two of three steps should be taken
    data0_1 = basic_logmgr.get_plot_data("t_wall", "t_wall", 0, 1)
    print(data0_1)
    assert len(data0_1) == 2

    # last two of three steps should be taken
    data1_2 = basic_logmgr.get_plot_data("t_wall", "t_wall", 1, 2)
    print(data1_2)
    assert len(data1_2) == 2
