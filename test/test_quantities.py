import pytest
import random

from time import sleep, monotonic as time_monotonic
from testlib import basicLogmgr
from logpyle import (
    LogManager,
    StepToStepDuration,
    TimestepCounter,
    WallTime,
    TimestepDuration,
    PushLogQuantity,
    CallableLogQuantityAdapter,
    ETA,
    GCStats,
    IntervalTimer,
    LogQuantity,
    PostLogQuantity,
    MultiLogQuantity,
    MultiPostLogQuantity,
)

# (name, value, unit, description, callFunc)
test_LogQuantity_types = [
        (
            "Quantity_name",
            1,
            "1",
            "Q init to 1",
            lambda x: x+1
         ),
        (
            "Quantity_name",
            1,
            None,
            "Q init to 1",
            lambda x: x+1
         ),
        (
            "Quantity_name",
            1,
            "1",
            None,
            lambda x: x+1
         ),
        (
            "Quantity_name",
            1,
            None,
            None,
            lambda x: x+1
         ),
        ]


@pytest.fixture(params=test_LogQuantity_types)
def customLogQuantity(request):
    q_name, value, unit, description, callFunc = request.param

    class TestLogQuantity(LogQuantity):
        def __init__(self, q_name, parameter) -> None:
            super().__init__(q_name, unit, description)
            setattr(self, q_name, parameter)
            self.func = callFunc

        def __call__(self):
            # gather quantities
            value = getattr(self, self.name)

            # update value every time quantity is called
            new_val = self.func(value)
            setattr(self, q_name, new_val)

            return new_val

    obj = TestLogQuantity(q_name, value)

    yield obj


def test_LogQuantity(basicLogmgr, customLogQuantity):
    basicLogmgr.add_quantity(customLogQuantity)

    init_value = getattr(customLogQuantity, customLogQuantity.name)

    predicted_list = []
    calculated_list = []

    N = 20

    # generate prediction
    predicted_list.append(init_value)
    for i in range(N):
        cur = predicted_list[-1]
        next = customLogQuantity.func(cur)
        predicted_list.append(next)

    for i in range(N):
        pre_val = getattr(customLogQuantity, customLogQuantity.name)
        assert pre_val == predicted_list[i]

        basicLogmgr.tick_before()
        # customLogQuantity should have been called
        middle_val = getattr(customLogQuantity, customLogQuantity.name)
        assert middle_val == predicted_list[i+1]
        basicLogmgr.tick_after()

        post_val = getattr(customLogQuantity, customLogQuantity.name)
        assert post_val == predicted_list[i+1]

        cur_vals = getattr(customLogQuantity, customLogQuantity.name)
        calculated_list.append(cur_vals)

    dataset = basicLogmgr.get_expr_dataset(customLogQuantity.name)
    stored_list = [tup[1] for tup in dataset[-1]]
    assert len(stored_list) == N

    print(calculated_list)
    print(stored_list)
    assert stored_list == calculated_list
    assert stored_list == predicted_list[1:]  # do not include initial vals


def test_LogQuantity_unimplemented(basicLogmgr: LogManager):
    quant = LogQuantity("q_one",)

    basicLogmgr.add_quantity(quant)

    with pytest.raises(NotImplementedError):
        basicLogmgr.tick_before()
        # do something
        basicLogmgr.tick_after()


@pytest.fixture(params=test_LogQuantity_types)
def customPostLogQuantity(request):
    q_name, value, unit, description, callFunc = request.param

    class TestPostLogQuantity(PostLogQuantity):
        def __init__(self, q_name, parameter) -> None:
            super().__init__(q_name, unit, description)
            setattr(self, q_name, parameter)
            self.func = callFunc

        def __call__(self):
            # gather quantities
            value = getattr(self, self.name)

            # update value every time quantity is called
            new_val = self.func(value)
            setattr(self, q_name, new_val)

            return new_val

    obj = TestPostLogQuantity(q_name, value)

    yield obj


def test_PostLogQuantity(basicLogmgr, customPostLogQuantity):
    basicLogmgr.add_quantity(customPostLogQuantity)

    init_value = getattr(customPostLogQuantity, customPostLogQuantity.name)

    predicted_list = []
    calculated_list = []

    N = 20

    # generate prediction
    predicted_list.append(init_value)
    for i in range(N):
        cur = predicted_list[-1]
        next = customPostLogQuantity.func(cur)
        predicted_list.append(next)

    for i in range(N):
        pre_val = getattr(customPostLogQuantity, customPostLogQuantity.name)
        assert pre_val == predicted_list[i]

        basicLogmgr.tick_before()
        # customPostLogQuantity should not have been called
        middle_val = getattr(customPostLogQuantity, customPostLogQuantity.name)
        assert middle_val == predicted_list[i]
        basicLogmgr.tick_after()

        post_val = getattr(customPostLogQuantity, customPostLogQuantity.name)
        assert post_val == predicted_list[i+1]

        cur_vals = getattr(customPostLogQuantity, customPostLogQuantity.name)
        calculated_list.append(cur_vals)

    dataset = basicLogmgr.get_expr_dataset(customPostLogQuantity.name)
    stored_list = [tup[1] for tup in dataset[-1]]
    assert len(stored_list) == N

    print(calculated_list)
    print(stored_list)
    assert stored_list == calculated_list
    assert stored_list == predicted_list[1:]  # do not include initial vals


# (names, values, units, descriptions, callFunc)
test_MultiLogQuantity_types = [
        (
            ["Quantity_1", "Quantity_2"],
            [1, 2],
            ["1", "1"],
            ["Q init to 1", "Q init to 2"],
            lambda x, y: [x+1, y+1]
         ),
        (
            ["Quantity_1", "Quantity_2"],
            [1, 2],
            [None, "1"],
            ["Q init to 1", "Q init to 2"],
            lambda x, y: [x+1, y+1]
         ),
        (
            ["Quantity_1", "Quantity_2"],
            [1, 2],
            ["1", None],
            ["Q init to 1", "Q init to 2"],
            lambda x, y: [x+1, y+1]
         ),
        (
            ["Quantity_1", "Quantity_2"],
            [1, 2],
            ["1", "1"],
            [None, "Q init to 2"],
            lambda x, y: [x+1, y+1]
         ),
        (
            ["Quantity_1", "Quantity_2"],
            [1, 2],
            ["1", "1"],
            ["Q init to 1", None],
            lambda x, y: [x+1, y+1]
         ),
        (
            ["Quantity_1", "Quantity_2"],
            [1, 2],
            None,
            ["Q init to 1", "Q init to 2"],
            lambda x, y: [x+1, y+1]
         ),
        (
            ["Quantity_1", "Quantity_2"],
            [1, 2],
            ["1", "1"],
            None,
            lambda x, y: [x+1, y+1]
         ),
        ]


@pytest.fixture(params=test_MultiLogQuantity_types)
def customMultiLogQuantity(request):
    names, values, units, descriptions, callFunc = request.param

    class TestLogQuantity(MultiLogQuantity):
        def __init__(self, names, parameters) -> None:
            super().__init__(names, units, descriptions)
            for name, parameter in zip(names, parameters):
                setattr(self, name, parameter)
            self.func = callFunc

        def __call__(self):
            # gather quantities
            values = [getattr(self, name) for name in self.names]

            # update value every time quantity is called
            new_vals = self.func(*values)
            for name, val in zip(self.names, new_vals):
                setattr(self, name, val)

            return new_vals

    obj = TestLogQuantity(names, values)

    yield obj


def test_MultiLogQuantity(basicLogmgr, customMultiLogQuantity):
    basicLogmgr.add_quantity(customMultiLogQuantity)

    init_values = []
    for name in customMultiLogQuantity.names:
        init_values.append(getattr(customMultiLogQuantity, name))

    predicted_list = []
    calculated_list = []

    N = 20

    # generate prediction
    predicted_list.append(init_values)
    for i in range(N):
        cur = predicted_list[-1]
        next = customMultiLogQuantity.func(*cur)
        predicted_list.append(next)

    for i in range(N):
        pre_vals = []
        for name in customMultiLogQuantity.names:
            pre_vals.append(getattr(customMultiLogQuantity, name))
        assert len(pre_vals) == len(init_values)
        assert pre_vals == predicted_list[i]

        basicLogmgr.tick_before()
        # customMultiLogQuantity should have been called
        middle_vals = []
        for name in customMultiLogQuantity.names:
            middle_vals.append(getattr(customMultiLogQuantity, name))
        assert len(middle_vals) == len(init_values)
        assert middle_vals == predicted_list[i+1]
        basicLogmgr.tick_after()

        post_vals = []
        for name in customMultiLogQuantity.names:
            post_vals.append(getattr(customMultiLogQuantity, name))
        assert len(post_vals) == len(init_values)
        assert post_vals == predicted_list[i+1]

        cur_vals = []
        for name in customMultiLogQuantity.names:
            cur_vals.append(getattr(customMultiLogQuantity, name))
        calculated_list.append(cur_vals)

    stored_list = [[] for i in range(N)]
    for name in customMultiLogQuantity.names:
        dataset = basicLogmgr.get_expr_dataset(name)
        tmp_stored_list = [tup[1] for tup in dataset[-1]]
        assert len(stored_list) == N
        for i, stored in enumerate(tmp_stored_list):
            stored_list[i].append(stored)

    print(calculated_list)
    print(stored_list)
    assert stored_list == calculated_list
    assert stored_list == predicted_list[1:]  # do not include initial vals


def test_MultiLogQuantity_unimplemented(basicLogmgr: LogManager):
    quant = MultiLogQuantity(["q_one", "q_two"], ["1", "1"], ["q1", "q2"])

    basicLogmgr.add_quantity(quant)

    with pytest.raises(NotImplementedError):
        basicLogmgr.tick_before()
        # do something
        basicLogmgr.tick_after()


@pytest.fixture(params=test_MultiLogQuantity_types)
def customMultiPostLogQuantity(request):
    names, values, units, descriptions, callFunc = request.param

    class TestLogQuantity(MultiPostLogQuantity):
        def __init__(self, names, parameters) -> None:
            super().__init__(names, units, descriptions)
            for name, parameter in zip(names, parameters):
                setattr(self, name, parameter)
            self.func = callFunc

        def __call__(self):
            # gather quantities
            values = [getattr(self, name) for name in self.names]

            # update value every time quantity is called
            new_vals = self.func(*values)
            for name, val in zip(self.names, new_vals):
                setattr(self, name, val)

            return new_vals

    obj = TestLogQuantity(names, values)

    yield obj


def test_MultiPostLogQuantity(basicLogmgr, customMultiPostLogQuantity):
    basicLogmgr.add_quantity(customMultiPostLogQuantity)

    init_values = []
    for name in customMultiPostLogQuantity.names:
        init_values.append(getattr(customMultiPostLogQuantity, name))

    predicted_list = []
    calculated_list = []

    N = 20

    # generate prediction
    predicted_list.append(init_values)
    for i in range(N):
        cur = predicted_list[-1]
        next = customMultiPostLogQuantity.func(*cur)
        predicted_list.append(next)

    for i in range(N):
        pre_vals = []
        for name in customMultiPostLogQuantity.names:
            pre_vals.append(getattr(customMultiPostLogQuantity, name))
        assert len(pre_vals) == len(init_values)
        assert pre_vals == predicted_list[i]

        basicLogmgr.tick_before()
        # customMultiPostLogQuantity should not have been called
        middle_vals = []
        for name in customMultiPostLogQuantity.names:
            middle_vals.append(getattr(customMultiPostLogQuantity, name))
        assert len(middle_vals) == len(init_values)
        assert middle_vals == predicted_list[i]
        basicLogmgr.tick_after()

        post_vals = []
        for name in customMultiPostLogQuantity.names:
            post_vals.append(getattr(customMultiPostLogQuantity, name))
        assert len(post_vals) == len(init_values)
        assert post_vals == predicted_list[i+1]

        cur_vals = []
        for name in customMultiPostLogQuantity.names:
            cur_vals.append(getattr(customMultiPostLogQuantity, name))
        calculated_list.append(cur_vals)

    stored_list = [[] for i in range(N)]
    for name in customMultiPostLogQuantity.names:
        dataset = basicLogmgr.get_expr_dataset(name)
        tmp_stored_list = [tup[1] for tup in dataset[-1]]
        assert len(stored_list) == N
        for i, stored in enumerate(tmp_stored_list):
            stored_list[i].append(stored)

    print(calculated_list)
    print(stored_list)
    assert stored_list == calculated_list
    assert stored_list == predicted_list[1:]  # do not include initial vals


def test_MultiPostLogQuantity_unimplemented(basicLogmgr: LogManager):
    quant = MultiPostLogQuantity(["q_one", "q_two"], ["1", "1"], ["q1", "q2"])

    basicLogmgr.add_quantity(quant)

    with pytest.raises(NotImplementedError):
        basicLogmgr.tick_before()
        # do something
        basicLogmgr.tick_after()


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


test_StepToStep_and_TimestepDuration_data = [
    (TimestepDuration("t_slp")),
    (StepToStepDuration("t_slp")),
]


@pytest.mark.parametrize("test_timer",
                         test_StepToStep_and_TimestepDuration_data)
def test_StepToStep_and_TimestepDuration_quantity(
        test_timer: any,
        basicLogmgr: LogManager
        ):
    tol = 0.005
    minTime = 0.02

    basicLogmgr.add_quantity(test_timer)

    N = 20

    sleep_times = [random.random() / 30 + minTime for i in range(N)]

    for i in range(N):
        if isinstance(test_timer, StepToStepDuration):
            sleep(sleep_times[i])

        basicLogmgr.tick_before()
        if isinstance(test_timer, TimestepDuration):
            sleep(sleep_times[i])
        basicLogmgr.tick_after()

    # first value is not defined for StepToStep, so we drop it
    if isinstance(test_timer, StepToStepDuration):
        del sleep_times[0]

    actual_times = [tup[1] for tup in basicLogmgr.get_expr_dataset("t_slp")[2]]
    print(actual_times, sleep_times)
    # assert that these quantities only differ by a max of tol
    # defined above
    for (predicted, actual) in zip(sleep_times, actual_times):
        assert abs(actual - predicted) < tol


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


def test_update_constants(basicLogmgr: LogManager):
    basicLogmgr.set_constant("value", 27)

    assert basicLogmgr.constants["value"] == 27

    basicLogmgr.tick_before()
    # do something ...
    assert basicLogmgr.constants["value"] == 27
    basicLogmgr.tick_after()

    assert basicLogmgr.constants["value"] == 27

    basicLogmgr.set_constant("value", 81)

    assert basicLogmgr.constants["value"] == 81

    basicLogmgr.tick_before()
    # do something ...
    assert basicLogmgr.constants["value"] == 81
    basicLogmgr.tick_after()

    assert basicLogmgr.constants["value"] == 81


def test_MultiLogQuantity_call_not_implemented(basicLogmgr: LogManager):
    multiLog = MultiLogQuantity(["q_one", "q_two"])
    basicLogmgr.add_quantity(multiLog)
    with pytest.raises(NotImplementedError):
        multiLog()


def test_IntervalTimer_subtimer(basicLogmgr: LogManager):
    tol = 0.1
    timer = IntervalTimer("timer")
    basicLogmgr.add_quantity(timer)

    expected_timer_list = []

    N = 20
    for i in range(N):
        good_sleep_time = (random.random()/10 + 0.1)
        bad_sleep_time = (random.random()/10 + 0.1)
        expected_timer_list.append(good_sleep_time)
        sub_timer = timer.get_sub_timer()

        basicLogmgr.tick_before()
        sub_timer.start()
        sleep(good_sleep_time)
        sub_timer.stop()
        sub_timer.submit()
        # do something
        sleep(bad_sleep_time)
        sleep(bad_sleep_time)
        basicLogmgr.tick_after()

    val = basicLogmgr.get_expr_dataset("timer")[-1]
    val_list = [data[1] for data in val]
    print(val_list)
    print(expected_timer_list)

    # enforce equality of durations
    for tup in zip(val_list, expected_timer_list):
        assert abs(tup[0] - tup[1]) < tol


def test_IntervalTimer_subtimer_blocking(basicLogmgr: LogManager):
    tol = 0.1
    timer = IntervalTimer("timer")
    basicLogmgr.add_quantity(timer)

    expected_timer_list = []

    N = 20
    for i in range(N):
        good_sleep_time = (random.random()/10 + 0.1)
        bad_sleep_time = (random.random()/10 + 0.1)
        expected_timer_list.append(good_sleep_time)
        sub_timer = timer.get_sub_timer()

        basicLogmgr.tick_before()
        with sub_timer:
            sleep(good_sleep_time)
        # do something
        sleep(bad_sleep_time)
        sleep(bad_sleep_time)
        basicLogmgr.tick_after()

    val = basicLogmgr.get_expr_dataset("timer")[-1]
    val_list = [data[1] for data in val]
    print(val_list)
    print(expected_timer_list)

    # enforce equality of durations
    for tup in zip(val_list, expected_timer_list):
        assert abs(tup[0] - tup[1]) < tol


def test_accurate_ETA_quantity(basicLogmgr: LogManager):
    # should begin calculation and ensure that the true time is
    # within a tolerance of the estimated time
    tol = 0.05

    N = 30

    test_timer = ETA(N+2, "t_fin")
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

    last = basicLogmgr.get_expr_dataset("t_fin")[-1][-1][-1]

    for i in range(N):
        basicLogmgr.tick_before()
        sleep(sleepTime)
        basicLogmgr.tick_after()

        actual_time = basicLogmgr.get_expr_dataset("t_fin")[-1][-1][-1]
        last = last - sleepTime
        print(last, actual_time)
        # assert that these quantities only
        # differ by a max of tol defined above
        assert abs(last - actual_time) < tol

def test_GCStats(basicLogmgr: LogManager):
    # will check if the example code breaks from using GCStats
    # should expand on later
    # currently ensures that after some time, GC from generation 1
    # eventually goes into generation 2
    gcStats = GCStats()
    basicLogmgr.add_quantity(gcStats)

    outerList = []

    last = None
    memoryHasChangedGenerations = False

    for istep in range(1000):
        basicLogmgr.tick_before()

        soonToBeLostRef = ['garb1', 'garb2', 'garb3'] * istep
        outerList.append(([soonToBeLostRef]))

        basicLogmgr.tick_after()

        sleep(0.02)

        cur = gcStats()
        # [enabled, # in generation1,  # in generation2, # in generation3,
        #  gen1 collections, gen1 collected, gen1 uncollected,
        #  gen2 collections, gen2 collected, gen2 uncollected,
        #  gen3 collections, gen3 collected, gen3 uncollected]
        print(cur)
        if last is not None and cur[2] > last[2]:
            memoryHasChangedGenerations = True
        last = cur

    assert memoryHasChangedGenerations

