import logging
import pytest
import random
from pymbolic.primitives import Variable
from random import uniform
from time import sleep, monotonic as time_monotonic
from warnings import warn

from logpyle import (
    add_run_info,
    add_general_quantities,
    add_simulation_quantities,
    set_dt,
    GCStats,
    LogManager,
    IntervalTimer,
    PushLogQuantity,
    TimestepDuration,
    StepToStepDuration,
    TimestepCounter,
    WallTime,
    LogQuantity,
    CallableLogQuantityAdapter,
    DtConsumer,
    ETA,
    EventCounter,
    time_and_count_function,
)


# Notes to self
#
# 1) Double enable/disable warnings/logging are not equivilent. Logging only
#       gives a warning for double enable, while warning throws a RuntimeError
#       for either double enable or double disable.
#       File as issue in logpyle
#


@pytest.fixture
def basicLogmgr():
    import os

    # setup
    filename = "THIS_LOG_SHOULD_BE_DELETED.sqlite"
    logmgr = LogManager(filename, "wo")
    # give obj to test
    yield logmgr
    # cleanup object
    logmgr.close()
    os.remove(filename)


def test_start_time_has_past(basicLogmgr: LogManager):
    assert basicLogmgr.start_time <= time_monotonic()


def test_empty_on_init(basicLogmgr: LogManager):
    # ensure that there are no initial watches
    assert len(basicLogmgr.watches) == 0


def test_basic_warning():
    with pytest.warns(UserWarning):
        warn("Oof. Something went awry.", UserWarning)


def test_logging_warnings_from_warnings_module(basicLogmgr: LogManager):
    first_warning_message = "Not a warning: First warning message!!!"
    first_warning_type = UserWarning

    basicLogmgr.tick_before()
    warn(first_warning_message, first_warning_type)
    basicLogmgr.tick_after()

    # ensure that the warning was caught properly
    print(basicLogmgr.warning_data[0])
    assert basicLogmgr.warning_data[0].message == first_warning_message
    assert basicLogmgr.warning_data[0].category == str(first_warning_type)
    assert basicLogmgr.warning_data[0].tick_count == 0

    second_warning_message = "Not a warning: Second warning message!!!"
    second_warning_type = UserWarning

    basicLogmgr.tick_before()
    warn(second_warning_message, second_warning_type)
    basicLogmgr.tick_after()

    # ensure that the warning was caught properly
    print(basicLogmgr.warning_data[1])
    assert basicLogmgr.warning_data[1].message == second_warning_message
    assert basicLogmgr.warning_data[1].category == str(second_warning_type)
    assert basicLogmgr.warning_data[1].tick_count == 1

    # save warnings to database
    basicLogmgr.save_warnings()

    # ensure that warnings are of the correct form
    message_ind = basicLogmgr.get_warnings().column_names.index("message")
    step_ind = basicLogmgr.get_warnings().column_names.index("step")
    data = basicLogmgr.get_warnings().data

    # ensure the first warning has been saved correctly
    assert data[0][message_ind] == first_warning_message
    assert data[0][step_ind] == 0

    # ensure the second warning has been saved correctly
    assert data[1][message_ind] == second_warning_message
    assert data[1][step_ind] == 1


def test_logging_warnings_from_logging_module(basicLogmgr: LogManager):
    logger = logging.getLogger(__name__)
    # logging.basicConfig() # required to log to terminal

    first_warning_message = "Not a warning: First warning message!!!"

    basicLogmgr.tick_before()
    logger.warning(first_warning_message)
    basicLogmgr.tick_after()

    # ensure that the warning was caught properly
    print(basicLogmgr.logging_data)
    assert basicLogmgr.logging_data[0].message == first_warning_message
    assert basicLogmgr.logging_data[0].category == "WARNING"
    assert basicLogmgr.logging_data[0].tick_count == 0

    second_warning_message = "Not a warning: Second warning message!!!"

    basicLogmgr.tick_before()
    logger.warning(second_warning_message)
    basicLogmgr.tick_after()

    # ensure that the warning was caught properly
    print(basicLogmgr.logging_data[1])
    assert basicLogmgr.logging_data[1].message == second_warning_message
    assert basicLogmgr.logging_data[1].category == "WARNING"
    assert basicLogmgr.logging_data[1].tick_count == 1

    # save warnings to database
    basicLogmgr.save_logging()

    # ensure that warnings are of the correct form
    message_ind = basicLogmgr.get_logging().column_names.index("message")
    step_ind = basicLogmgr.get_logging().column_names.index("step")
    data = basicLogmgr.get_logging().data

    # ensure the first warning has been saved correctly
    assert data[0][message_ind] == first_warning_message
    assert data[0][step_ind] == 0

    # ensure the second warning has been saved correctly
    assert data[1][message_ind] == second_warning_message
    assert data[1][step_ind] == 1


def test_accurate_TimestepCounter_quantity(basicLogmgr: LogManager):
    test_timer = TimestepCounter("t_step_count")
    basicLogmgr.add_quantity(test_timer)

    n1 = 200
    n2 = 120

    for i in range(n1):
        basicLogmgr.tick_before()
        # do something ...
        basicLogmgr.tick_after()
    assert basicLogmgr.last_values["t_step_count"] == n1 - 1

    for i in range(n2):
        basicLogmgr.tick_before()
        # do something ...
        basicLogmgr.tick_after()
    assert basicLogmgr.last_values["t_step_count"] == n1 + n2 - 1


def test_accurate_StepToStepDuration_quantity(basicLogmgr: LogManager):
    tol = 0.005
    minTime = 0.02

    test_timer = StepToStepDuration("t_slp")
    basicLogmgr.add_quantity(test_timer)

    for i in range(20):
        basicLogmgr.tick_before()
        # do something ...
        sleepTime = random.random() / 30 + minTime
        basicLogmgr.tick_after()

        sleep(sleepTime)

        basicLogmgr.tick_before()
        # assert that these quantities only differ by a max of tol
        # defined above
        print(sleepTime, test_timer())
        assert abs(test_timer() - sleepTime) < tol
        # do something ...
        basicLogmgr.tick_after()


def test_accurate_TimestepDuration_quantity(basicLogmgr: LogManager):
    tol = 0.005
    minTime = 0.02

    test_timer = TimestepDuration("t_slp")
    basicLogmgr.add_quantity(test_timer)

    for i in range(20):
        sleepTime = random.random() / 30 + minTime

        basicLogmgr.tick_before()
        sleep(sleepTime)
        basicLogmgr.tick_after()

        actual_time = basicLogmgr.get_expr_dataset("t_slp")[2][-1][1]
        # assert that these quantities only differ by a max of tol
        # defined above
        print(sleepTime, actual_time)
        assert abs(actual_time - sleepTime) < tol


def test_accurate_WallTime_quantity(basicLogmgr: LogManager):
    tol = 0.1
    minTime = 0.02

    N = 20

    test_timer = WallTime("t_total")
    startTime = time_monotonic()

    basicLogmgr.add_quantity(test_timer)
    for i in range(N):
        sleepBeforeTime = random.random() / 30 + minTime
        sleepDuringTime = random.random() / 30 + minTime

        sleep(sleepBeforeTime)

        basicLogmgr.tick_before()
        sleep(sleepDuringTime)
        basicLogmgr.tick_after()

        now = time_monotonic()
        totalTime = now - startTime
        actual_time = basicLogmgr.get_expr_dataset("t_total")[-1][-1][-1]
        print(totalTime, actual_time)
        # assert that these quantities only differ by a max of tol
        # defined above
        assert abs(totalTime - actual_time) < tol


def test_basic_Push_Log_quantity(basicLogmgr: LogManager):
    pushQuantity = PushLogQuantity("pusher")
    basicLogmgr.add_quantity(pushQuantity)

    for i in range(20):
        pushQuantity.push_value(i)
        basicLogmgr.tick_before()
        # do something ...
        basicLogmgr.tick_after()
        print(basicLogmgr.get_expr_dataset("pusher"))
        assert basicLogmgr.get_expr_dataset("pusher")[-1][-1][-1] == i


def test_double_push_Push_Log_quantity(basicLogmgr: LogManager):
    pushQuantity = PushLogQuantity("pusher")
    basicLogmgr.add_quantity(pushQuantity)

    firstVal = 25
    secondVal = 36

    pushQuantity.push_value(firstVal)
    with pytest.raises(RuntimeError):
        pushQuantity.push_value(secondVal)


def test_general_quantities(basicLogmgr: LogManager):
    # verify that exactly all general quantities were added

    add_general_quantities(basicLogmgr)
    basicLogmgr.tick_before()
    basicLogmgr.tick_after()
    basicLogmgr.save()

    idealQuantitiesAdded = [
        "t_step",
        "t_wall",
        "t_2step",
        "t_log",
        "memory_usage_hwm",
        "step",
        "t_init",
    ]

    actualQuantitiesAdded = basicLogmgr.db_conn.execute(
        "select * from quantities"
    ).fetchall()

    # reformat into list of quantities
    actualQuantitiesAdded = [desc[0] for desc in actualQuantitiesAdded]

    # sort lists for comparison
    idealQuantitiesAdded.sort()
    actualQuantitiesAdded.sort()

    for i in range(len(idealQuantitiesAdded)):
        assert idealQuantitiesAdded[i] == actualQuantitiesAdded[i]

    assert len(idealQuantitiesAdded) == len(actualQuantitiesAdded)


def test_simulation_quantities(basicLogmgr: LogManager):
    add_simulation_quantities(basicLogmgr)

    # must set a dt for simulation quantities
    set_dt(basicLogmgr, 0.05)

    basicLogmgr.tick_before()
    sleep(0.01)
    basicLogmgr.tick_after()
    basicLogmgr.save()

    idealQuantitiesAdded = [
        "t_sim",
        "dt",
    ]

    actualQuantitiesAdded = basicLogmgr.db_conn.execute(
        "select * from quantities"
    ).fetchall()

    # reformat into list of quantities
    actualQuantitiesAdded = [desc[0] for desc in actualQuantitiesAdded]

    # sort lists for comparison
    idealQuantitiesAdded.sort()
    actualQuantitiesAdded.sort()

    for i in range(len(idealQuantitiesAdded)):
        assert idealQuantitiesAdded[i] == actualQuantitiesAdded[i]

    assert len(idealQuantitiesAdded) == len(actualQuantitiesAdded)


def test_nonexisting_table(basicLogmgr: LogManager):
    add_general_quantities(basicLogmgr)

    with pytest.raises(KeyError):
        basicLogmgr.get_table("nonexistent table")


def test_existing_database_no_overwrite():
    import os

    filename = "THIS_LOG_SHOULD_BE_DELETED.sqlite"
    logmgr = LogManager(filename, "wo")
    add_general_quantities(logmgr)
    logmgr.tick_before()
    logmgr.tick_after()

    logmgr.save()
    logmgr.close()

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

    os.remove(filename)


def test_open_existing_database():
    import os

    filename = "THIS_LOG_SHOULD_BE_DELETED.sqlite"
    logmgr = LogManager(filename, "wo")

    add_general_quantities(logmgr)

    firstTick = logmgr.get_expr_dataset("t_wall")
    print(firstTick)

    logmgr.tick_before()
    logmgr.tick_after()

    secondTick = logmgr.get_expr_dataset("t_wall")
    print(secondTick)

    logmgr.save()
    logmgr.close()

    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED.sqlite", "r")
    # expect the data to be the same as the second tick
    savedData = logmgr.get_expr_dataset("t_wall")
    print(savedData)
    assert savedData == secondTick

    os.remove(filename)


# assuming that a nameless (in memory) database should not save
# data after closing.
def test_nameless_LogManager():
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


def test_unique_suffix():
    # testing two in a row with no computation in between should force
    # a collision due to the names being based on time of day
    import os

    def isUniqueFilename(str: str):
        if str.startswith("THIS_LOG_SHOULD_BE_DELETED-"):
            return True
        else:
            return False

    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED.sqlite", "wu")
    logmgr.close()

    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED.sqlite", "wu")
    logmgr.close()

    # assert that two distinct databases were created
    files = [f for f in os.listdir() if isUniqueFilename(f)]
    print(files)
    assert len(files) == 2

    os.remove(files[0])
    os.remove(files[1])


def test_read_nonexistant_database():
    import os

    fakeFileName = "THIS_LOG_SHOULD_BE_DELETED_AND_DOES_NOT_EXIST"
    with pytest.raises(RuntimeError):
        LogManager(fakeFileName, "r")

    os.remove(fakeFileName)


def test_add_run_info(basicLogmgr: LogManager):
    from socket import gethostname

    timeTol = 0.5

    add_run_info(basicLogmgr)

    savedMachine = basicLogmgr.constants["machine"]
    print(savedMachine)
    assert savedMachine == gethostname()

    from time import time

    # ensure that it is the same day that this log was created
    savedDate = basicLogmgr.constants["date"]
    print(savedDate)

    savedTime = basicLogmgr.constants["unixtime"]
    print(savedTime)
    assert abs(time() - savedTime) < timeTol


def test_unimplemented_logging_quantity(basicLogmgr: LogManager):
    # LogQuantity is an abstract interface and should not be called
    with pytest.raises(NotImplementedError):
        test_timer = LogQuantity("t_step_count")
        basicLogmgr.add_quantity(test_timer)

        basicLogmgr.tick_before()
        # do something ...
        basicLogmgr.tick_after()


def test_GCStats(basicLogmgr: LogManager):
    # TODO
    # will check if the example code breaks from using GCStats
    # should expand on later
    pass


def test_set_dt(basicLogmgr: LogManager):
    # Should verify that the dt is set/changed and is applied
    # to dt consuming quantities after changing
    add_simulation_quantities(basicLogmgr)
    for descriptor_list in [
        basicLogmgr.before_gather_descriptors,
        basicLogmgr.after_gather_descriptors,
    ]:
        for descriptor in descriptor_list:
            q_dt = descriptor.quantity.dt
            print(q_dt)
            assert q_dt is None

    set_dt(basicLogmgr, 0.5)

    for descriptor_list in [
        basicLogmgr.before_gather_descriptors,
        basicLogmgr.after_gather_descriptors,
    ]:
        for descriptor in descriptor_list:
            q_dt = descriptor.quantity.dt
            print(q_dt)
            assert q_dt is not None
            assert q_dt == 0.5

    set_dt(basicLogmgr, 0.02)

    for descriptor_list in [
        basicLogmgr.before_gather_descriptors,
        basicLogmgr.after_gather_descriptors,
    ]:
        for descriptor in descriptor_list:
            q_dt = descriptor.quantity.dt
            print(q_dt)
            assert q_dt is not None
            assert q_dt == 0.02


def test_CallableLogQuantity(basicLogmgr: LogManager):
    global counter
    counter = 0

    def calledFunc() -> float:
        global counter
        counter += 1
        return random.random()

    callable = CallableLogQuantityAdapter(calledFunc, "caller")
    basicLogmgr.add_quantity(callable)

    N = 50
    for i in range(N):
        basicLogmgr.tick_before()
        # do something ...
        basicLogmgr.tick_after()

    print(counter)
    assert counter == N


def test_MultiLogQuantity(basicLogmgr: LogManager):
    # TODO
    pass


def test_MultiPostLogQuantity(basicLogmgr: LogManager):
    # TODO
    pass


def test_double_enable_warnings(basicLogmgr: LogManager):
    # default is enabled
    with pytest.raises(RuntimeError):
        basicLogmgr.capture_warnings(True)


def test_double_disable_warnings(basicLogmgr: LogManager):
    # default is enabled
    basicLogmgr.capture_warnings(False)
    with pytest.raises(RuntimeError):
        basicLogmgr.capture_warnings(False)


# # TODO
# # currently not raising
# def test_double_enable_logging(basicLogmgr: LogManager):
#     # TODO
#     # default is enabled
#     with pytest.raises(RuntimeError):
#         basicLogmgr.capture_logging(True)


# # TODO
# # currently not raising
# def test_double_disable_logging(basicLogmgr: LogManager):
#     # TODO
#     # default is enabled
#     basicLogmgr.capture_logging(False)
#     with pytest.raises(RuntimeError):
#         basicLogmgr.capture_logging(False)


def test_double_add_quantity(basicLogmgr: LogManager):
    class Fifteen(LogQuantity):
        def __call__(self) -> int:
            return 15

    class FifteenStr(LogQuantity):
        def __call__(self) -> int:
            return "15.0"

    basicLogmgr.add_quantity(Fifteen("fifteen"))
    with pytest.raises(RuntimeError):
        basicLogmgr.add_quantity(Fifteen("fifteen"))


def test_add_watches(basicLogmgr: LogManager):
    # test adding a few watches

    class Fifteen(LogQuantity):
        def __call__(self) -> int:
            return 15

    class FifteenStr(LogQuantity):
        def __call__(self) -> int:
            return "15.0"

    basicLogmgr.add_quantity(Fifteen("name1"))
    basicLogmgr.add_quantity(Fifteen("name2"))
    basicLogmgr.add_quantity(FifteenStr("tup_name1"))

    watch_list = ["name1", ("tup_name1", "str"), "name2"]

    basicLogmgr.add_watches(watch_list)

    basicLogmgr.tick_before()
    # do something ...
    basicLogmgr.tick_before()
    basicLogmgr.save()

    # check that all watches are present
    actualWatches = [watch.expr for watch in basicLogmgr.watches]
    expected = ["name1", "tup_name1", "name2"]
    actualWatches.sort()
    expected.sort()
    print(actualWatches, expected)
    assert actualWatches == expected


def test_IntervalTimer_subtimer():
    # TODO
    pass


def test_time_and_count_function(basicLogmgr: LogManager):
    # TODO
    tol = 0.1

    def func_to_be_timed(t: float):
        sleep(t)
        return

    timer = IntervalTimer("t_duration")
    counter = EventCounter("num_itr")

    basicLogmgr.add_quantity(counter)

    basicLogmgr.tick_before()

    N = 10
    for i in range(N):
        time_and_count_function(func_to_be_timed, timer, counter)(0.1)

    basicLogmgr.tick_after()

    print(timer.elapsed, counter.events)

    assert abs(timer.elapsed - N * 0.1) < tol
    assert counter.events == N


# # TODO currently calls unimplemented function
# def test_EventCounter(basicLogmgr: LogManager):
#     # the event counter should keep track of events that occur
#     # during the timestep

#     counter1 = EventCounter("num_events1")
#     counter2 = EventCounter("num_events2")

#     basicLogmgr.add_quantity(counter1)

#     N = 21
#     basicLogmgr.tick_before()

#     for i in range(N):
#         counter1.add()

#     print(counter1.events)
#     assert counter1.events == N

#     basicLogmgr.tick_after()

#     # transfer counter1's count to counter2's
#     basicLogmgr.tick_before()

#     # at the beggining of tick, counter should clear
#     print(counter1.events)
#     assert counter1.events == 0

#     for i in range(N):
#         if i % 3 == 0:
#             counter1.add()

#     counter2.transfer(counter1)

#     assert counter1.events == 0
#     assert counter2.events == N

#     for i in range(N):
#         if i % 3 == 0:
#             counter2.add()

#     print(counter2.events)
#     assert counter2.events == 2 * N

#     basicLogmgr.tick_after()


def test_joint_dataset(basicLogmgr: LogManager):
    add_general_quantities(basicLogmgr)
    basicLogmgr.tick_before()
    basicLogmgr.tick_after()
    basicLogmgr.save()

    idealQuantitiesAdded = [
        "t_step",
        ("cpu time", Variable("s"), "t_wall"),
        "memory_usage_hwm",
        "t_init",
    ]
    quantityNames = [
        "t_step",
        "cpu time",
        "memory_usage_hwm",
        "t_init",
    ]
    dataset = basicLogmgr.get_joint_dataset(idealQuantitiesAdded)

    print(dataset)
    names = list(dataset[0])
    names.sort()
    quantityNames.sort()

    for quantity in quantityNames:
        assert quantity in names
    assert len(names) == len(quantityNames)


def test_plot_data(basicLogmgr: LogManager):
    add_general_quantities(basicLogmgr)

    basicLogmgr.tick_before()
    # do something ...
    basicLogmgr.tick_after()

    # there should be one step
    data1 = basicLogmgr.get_plot_data("t_wall", "t_wall")
    print(data1)
    assert len(data1[0][0]) == 1

    basicLogmgr.tick_before()
    # do something ...
    basicLogmgr.tick_after()

    # there should be two steps
    data2 = basicLogmgr.get_plot_data("t_wall", "t_wall")
    print(data2)
    assert len(data2[0][0]) == 2

    basicLogmgr.tick_before()
    # do something ...
    basicLogmgr.tick_after()

    # there should be three steps
    data3 = basicLogmgr.get_plot_data("t_wall", "t_wall")
    print(data3)
    assert len(data3[0][0]) == 3

    # first two of three steps should be taken
    data0_1 = basicLogmgr.get_plot_data("t_wall", "t_wall", 0, 1)
    print(data0_1)
    assert len(data0_1) == 2

    # last two of three steps should be taken
    data1_2 = basicLogmgr.get_plot_data("t_wall", "t_wall", 1, 2)
    print(data1_2)
    assert len(data1_2) == 2


# # TODO currently crashes when no timesteps are present
# def test_empty_plot_data(basicLogmgr: LogManager):
#     add_general_quantities(basicLogmgr)

#     # there should be zero step
#     data0 = basicLogmgr.get_plot_data("t_wall", "t_wall")
#     print(data0)
#     assert len(data0[0][0]) == 0

#     basicLogmgr.tick_before()
#     # do something ...
#     basicLogmgr.tick_after()

#     # there should be one step
#     data1 = basicLogmgr.get_plot_data("t_wall", "t_wall")
#     print(data1)
#     assert len(data1[0][0]) == 1

#     basicLogmgr.tick_before()
#     # do something ...
#     basicLogmgr.tick_after()

#     # there should be two steps
#     data2 = basicLogmgr.get_plot_data("t_wall", "t_wall")
#     print(data2)
#     assert len(data2[0][0]) == 2

#     basicLogmgr.tick_before()
#     # do something ...
#     basicLogmgr.tick_after()

#     # there should be three steps
#     data3 = basicLogmgr.get_plot_data("t_wall", "t_wall")
#     print(data3)
#     assert len(data3[0][0]) == 3

#     # first two of three steps should be taken
#     data0_1 = basicLogmgr.get_plot_data("t_wall", "t_wall", 0, 1)
#     print(data0_1)
#     assert len(data0_1) == 2

#     # last two of three steps should be taken
#     data1_2 = basicLogmgr.get_plot_data("t_wall", "t_wall", 1, 2)
#     print(data1_2)
#     assert len(data1_2) == 2


def test_write_datafile(basicLogmgr: LogManager):
    import os

    def hasContents(str1):
        trimedStr = str1.strip()
        if len(trimedStr) == 0:
            return False
        else:
            return True

    add_general_quantities(basicLogmgr)

    N = 20

    for i in range(N):
        basicLogmgr.tick_before()
        # do something ...
        basicLogmgr.tick_after()

    filename = "THIS_LOG_SHOULD_BE_DELETED.txt"
    # filename = "dataout.txt"

    basicLogmgr.write_datafile(filename, "t_wall", "t_wall")

    File_object = open(filename, "r")
    lines = File_object.readlines()
    lines = filter(hasContents, lines)

    i = 0
    for line in lines:
        print(line)
        i += 1

    print(i)
    assert i == N

    os.remove(filename)


def test_plot_matplotlib(basicLogmgr: LogManager):
    add_general_quantities(basicLogmgr)

    N = 20

    for i in range(N):
        basicLogmgr.tick_before()
        # do something ...
        basicLogmgr.tick_after()

    basicLogmgr.plot_matplotlib("t_wall", "t_wall")


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
def test_single_rank_aggregator(basicLogmgr, agg, data, expected):
    add_general_quantities(basicLogmgr)

    pushQ = PushLogQuantity("value")
    basicLogmgr.add_quantity(pushQ)

    for val in data:
        print(val)
        pushQ.push_value(val)
        basicLogmgr.tick_before()
        # do something ...
        basicLogmgr.tick_after()

    basicLogmgr.save()

    # NOT_REAL_AGG should raise an error
    if agg == "NOT_REAL_AGG":
        with pytest.raises(ValueError):
            result = basicLogmgr.get_expr_dataset("value." + agg)
        return

    # nothing else should raise an error
    result = basicLogmgr.get_expr_dataset("value." + agg)
    print(result)
    assert result[-1][-1][-1] == expected


# -------------------- Time Intensive Tests --------------------


def test_accurate_ETA_quantity(basicLogmgr: LogManager):
    # should begin calculation and ensure that the true time is
    # within a tolerance of the estimated time
    tol = 0.3

    test_timer = ETA(50, "t_fin")
    basicLogmgr.add_quantity(test_timer)

    sleepTime = 0.1

    # add first tick
    basicLogmgr.tick_before()
    sleep(sleepTime)
    basicLogmgr.tick_after()

    # add second tick
    basicLogmgr.tick_before()
    sleep(sleepTime)
    basicLogmgr.tick_after()

    N = 30
    last = basicLogmgr.get_expr_dataset("t_fin")[-1][-1][-1]

    for i in range(N):
        basicLogmgr.tick_before()
        sleep(sleepTime)
        basicLogmgr.tick_after()

        actual_time = basicLogmgr.get_expr_dataset("t_fin")[-1][-1][-1]
        print(last, actual_time)
        # assert that these quantities only
        # differ by a max of tol defined above
        if i > 5:  # dont expect the first couple to be accurate
            assert abs(last - actual_time) < tol
        last = last - sleepTime


def test_MemoryHwm_quantity(basicLogmgr: LogManager):
    # can only check if nothing breaks and the watermark never lowers,
    # as we do not know what else is on the system

    # set a run property
    basicLogmgr.set_constant("myconst", uniform(0, 1))

    # Generic run metadata, such as command line, host, and time
    add_run_info(basicLogmgr)

    # Time step duration, wall time, ...
    add_general_quantities(basicLogmgr)

    # Simulation time, time step
    add_simulation_quantities(basicLogmgr)

    # Additional quantities to log
    vis_timer = IntervalTimer("t_vis", "Time spent visualizing")
    basicLogmgr.add_quantity(vis_timer)
    basicLogmgr.add_quantity(GCStats())

    # Watches are printed periodically during execution
    basicLogmgr.add_watches(
        [
            "step.max",
            "t_sim.max",
            "t_step.max",
            "t_vis",
            "t_log",
            "memory_usage_hwm",
            "myconst",
        ]
    )

    for istep in range(200):
        basicLogmgr.tick_before()

        dt = uniform(0.01, 0.05)
        set_dt(basicLogmgr, dt)
        sleep(dt)

        # Illustrate custom timers
        if istep % 10 == 0:
            with vis_timer.start_sub_timer():
                sleep(0.05)

        if istep == 50:
            print("FYI: Setting watch interval to 5 seconds.")
            basicLogmgr.set_watch_interval(5)

        if istep == 150:
            print("FYI: Setting watch interval back to 1 second.")
            basicLogmgr.set_watch_interval(1)

        basicLogmgr.tick_after()
