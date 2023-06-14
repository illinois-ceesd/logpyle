import logging
import pytest
import random
import mpi4py
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
    ETA,
)


# Notes to self
#
# 1) Might want to add documentation to PushLogQuantity specifying that it
# 		keeps track of quantities pushed outside of tick time interval.
#


def test_example():
    assert True


def test_start_time_has_past():
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")
    assert logmgr.start_time <= time_monotonic()
    logmgr.close()


def test_empty_on_init():
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    # ensure that there are no initial watches
    assert len(logmgr.watches) == 0

    logmgr.close()


def test_basic_warning():
    with pytest.warns(UserWarning):
        warn("Oof. Something went awry.", UserWarning)


def test_logging_warnings_from_warnings_module():
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    first_warning_message = "Not a warning: First warning message!!!"
    first_warning_type = UserWarning

    logmgr.tick_before()
    warn(first_warning_message, first_warning_type)
    logmgr.tick_after()

    # ensure that the warning was caught properly
    print(logmgr.warning_data[0])
    assert logmgr.warning_data[0].message == first_warning_message
    assert logmgr.warning_data[0].category == str(first_warning_type)
    assert logmgr.warning_data[0].tick_count == 0

    second_warning_message = "Not a warning: Second warning message!!!"
    second_warning_type = UserWarning

    logmgr.tick_before()
    warn(second_warning_message, second_warning_type)
    logmgr.tick_after()

    # ensure that the warning was caught properly
    print(logmgr.warning_data[1])
    assert logmgr.warning_data[1].message == second_warning_message
    assert logmgr.warning_data[1].category == str(second_warning_type)
    assert logmgr.warning_data[1].tick_count == 1

    # save warnings to database
    logmgr.save_warnings()

    # ensure that warnings are of the correct form
    message_ind = logmgr.get_warnings().column_names.index("message")
    step_ind = logmgr.get_warnings().column_names.index("step")
    data = logmgr.get_warnings().data

    # ensure the first warning has been saved correctly
    assert data[0][message_ind] == first_warning_message
    assert data[0][step_ind] == 0

    # ensure the second warning has been saved correctly
    assert data[1][message_ind] == second_warning_message
    assert data[1][step_ind] == 1

    logmgr.close()


def test_logging_warnings_from_logging_module():
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    logger = logging.getLogger(__name__)
    # logging.basicConfig() # required to log to terminal

    first_warning_message = "Not a warning: First warning message!!!"

    logmgr.tick_before()
    logger.warning(first_warning_message)
    logmgr.tick_after()

    # ensure that the warning was caught properly
    # print(logmgr.save_logging())
    print(logmgr.logging_data)
    assert logmgr.logging_data[0].message == first_warning_message
    assert logmgr.logging_data[0].category == "WARNING"
    assert logmgr.logging_data[0].tick_count == 0

    second_warning_message = "Not a warning: Second warning message!!!"

    logmgr.tick_before()
    logger.warning(second_warning_message)
    logmgr.tick_after()

    # ensure that the warning was caught properly
    print(logmgr.logging_data[1])
    assert logmgr.logging_data[1].message == second_warning_message
    assert logmgr.logging_data[1].category == "WARNING"
    assert logmgr.logging_data[1].tick_count == 1

    # save warnings to database
    logmgr.save_logging()

    # ensure that warnings are of the correct form
    message_ind = logmgr.get_logging().column_names.index("message")
    step_ind = logmgr.get_logging().column_names.index("step")
    data = logmgr.get_logging().data

    # ensure the first warning has been saved correctly
    assert data[0][message_ind] == first_warning_message
    assert data[0][step_ind] == 0

    # ensure the second warning has been saved correctly
    assert data[1][message_ind] == second_warning_message
    assert data[1][step_ind] == 1

    logmgr.close()


def test_accurate_TimestepCounter_quantity():
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    test_timer = TimestepCounter("t_step_count")
    logmgr.add_quantity(test_timer)

    n1 = 200
    n2 = 120

    for i in range(n1):
        logmgr.tick_before()
        # do something ...
        logmgr.tick_after()
    assert logmgr.last_values["t_step_count"] == n1 - 1

    for i in range(n2):
        logmgr.tick_before()
        # do something ...
        logmgr.tick_after()
    assert logmgr.last_values["t_step_count"] == n1 + n2 - 1

    logmgr.close()


def test_accurate_StepToStepDuration_quantity():
    tol = 0.005
    minTime = 0.02
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    test_timer = StepToStepDuration("t_slp")
    logmgr.add_quantity(test_timer)

    for i in range(20):
        logmgr.tick_before()
        # do something ...
        sleepTime = random.random() / 30 + minTime
        logmgr.tick_after()

        sleep(sleepTime)

        logmgr.tick_before()
        # assert that these quantities only differ by a max of tol
        # defined above
        print(sleepTime, test_timer())
        assert abs(test_timer() - sleepTime) < tol
        # do something ...
        logmgr.tick_after()

    logmgr.close()


def test_accurate_TimestepDuration_quantity():
    tol = 0.005
    minTime = 0.02
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    test_timer = TimestepDuration("t_slp")
    logmgr.add_quantity(test_timer)

    for i in range(20):
        sleepTime = random.random() / 30 + minTime

        logmgr.tick_before()
        sleep(sleepTime)
        logmgr.tick_after()

        actual_time = logmgr.get_expr_dataset("t_slp")[2][-1][1]
        # assert that these quantities only differ by a max of tol
        # defined above
        print(sleepTime, actual_time)
        assert abs(actual_time - sleepTime) < tol

    logmgr.close()
    pass


def test_accurate_WallTime_quantity():
    tol = 0.1
    minTime = 0.02
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    N = 20

    test_timer = WallTime("t_total")
    startTime = time_monotonic()

    logmgr.add_quantity(test_timer)
    for i in range(N):
        sleepBeforeTime = random.random() / 30 + minTime
        sleepDuringTime = random.random() / 30 + minTime

        sleep(sleepBeforeTime)

        logmgr.tick_before()
        sleep(sleepDuringTime)
        logmgr.tick_after()

        now = time_monotonic()
        totalTime = now - startTime
        actual_time = logmgr.get_expr_dataset("t_total")[-1][-1][-1]
        print(totalTime, actual_time)
        # assert that these quantities only differ by a max of tol
        # defined above
        assert abs(totalTime - actual_time) < tol

    logmgr.close()
    pass


def test_basic_Push_Log_quantity():
    # TODO
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    pushQuantity = PushLogQuantity("pusher")
    logmgr.add_quantity(pushQuantity)

    for i in range(20):
        pushQuantity.push_value(i)
        logmgr.tick_before()
        # do something ...
        logmgr.tick_after()
        print(logmgr.get_expr_dataset("pusher"))
        assert logmgr.get_expr_dataset("pusher")[-1][-1][-1] == i

    logmgr.close()
    pass


def test_double_push_Push_Log_quantity():
    # TODO
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    pushQuantity = PushLogQuantity("pusher")
    logmgr.add_quantity(pushQuantity)

    firstVal = 25
    secondVal = 36

    pushQuantity.push_value(firstVal)
    with pytest.raises(RuntimeError):
        pushQuantity.push_value(secondVal)
        logmgr.tick_before()
        # do something ...
        logmgr.tick_after()
        assert logmgr.get_expr_dataset("pusher")[-1][-1][-1] == firstVal

    logmgr.close()


def test_accurate_InitTime_quantity():
    # TODO
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    logmgr.close()
    pass


def test_general_quantities():
    # TODO
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    logmgr.close()
    pass


def test_simulation_quantities():
    # TODO
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    logmgr.close()
    pass


def test_add_run_info():
    # TODO
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    logmgr.close()
    pass


def test_accurate_BLANK_quantity():
    # TODO
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    logmgr.close()
    pass


def test_unimplemented_logging_quantity():
    # TODO
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    # LogQuantity is an abstract interface and should not be called
    with pytest.raises(NotImplementedError):
        test_timer = LogQuantity("t_step_count")
        logmgr.add_quantity(test_timer)

        logmgr.tick_before()
        # do something ...
        logmgr.tick_after()

    logmgr.close()
    pass


def test_GCStats():
    # TODO
    # will check if the example code breaks from using GCStats
    # should expand on later
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    logmgr.close()
    pass


def test_set_dt():
    # TODO
    # Should verify that the dt is changed and is applied to dt consuming quantities after changing
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    logmgr.close()
    pass


def test_MultiPostLogQuantity():
    # TODO
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    logmgr.close()
    pass


def test_add_watches():
    # TODO
    # test adding a few watches
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    logmgr.close()
    pass


def test_nameless_LogManager():
    # TODO
    logmgr = LogManager(None, "wo")

    logmgr.close()
    pass


def test_unique_suffix():
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED.sqlite", "wu")
    logmgr.close()
    pass


def test_read_nonexistant_database():
    with pytest.raises(RuntimeError):
        logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED_AND_DOES_NOT_EXIST", "r")


def test_time_and_count_function():
    # TODO
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    logmgr.close()
    pass


def test_accurate_BLANK_quantity():
    # TODO
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    logmgr.close()
    pass


# -------------------- Time Intensive Tests --------------------


def test_accurate_ETA_quantity():
    # should begin calculation and ensure that the true time is within a tolerance of the estimated time
    tol = 0.2
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    test_timer = ETA(50, "t_fin")
    logmgr.add_quantity(test_timer)

    sleepTime = 0.1

    logmgr.tick_before()
    sleep(sleepTime)
    logmgr.tick_after()

    N = 50
    last = (N - 1) * sleepTime

    for i in range(N):
        logmgr.tick_before()
        sleep(sleepTime)
        logmgr.tick_after()

        actual_time = logmgr.get_expr_dataset("t_fin")[-1][-1][-1]
        print(
            last, actual_time
        )  # assert that these quantities only differ by a max of tol defined above
        assert abs(last - actual_time) < tol
        last = last - sleepTime

    logmgr.close()
    pass


def test_MemoryHwm_quantity():
    # TODO
    # can only check if nothing breaks and the watermark never lowers, as we do not know what else is on the system
    logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")

    # set a run property
    logmgr.set_constant("myconst", uniform(0, 1))

    # Generic run metadata, such as command line, host, and time
    add_run_info(logmgr)

    # Time step duration, wall time, ...
    add_general_quantities(logmgr)

    # Simulation time, time step
    add_simulation_quantities(logmgr)

    # Additional quantities to log
    vis_timer = IntervalTimer("t_vis", "Time spent visualizing")
    logmgr.add_quantity(vis_timer)
    logmgr.add_quantity(GCStats())

    # Watches are printed periodically during execution
    logmgr.add_watches(
        ["step.max", "t_sim.max", "t_step.max", "t_vis", "t_log", "memory_usage_hwm"]
    )

    for istep in range(200):
        logmgr.tick_before()

        dt = uniform(0.01, 0.05)
        set_dt(logmgr, dt)
        sleep(dt)

        # Illustrate custom timers
        if istep % 10 == 0:
            with vis_timer.start_sub_timer():
                sleep(0.05)

        if istep == 50:
            print("FYI: Setting watch interval to 5 seconds.")
            logmgr.set_watch_interval(5)

        if istep == 150:
            print("FYI: Setting watch interval back to 1 second.")
            logmgr.set_watch_interval(1)

        logmgr.tick_after()

    logmgr.close()
    pass
