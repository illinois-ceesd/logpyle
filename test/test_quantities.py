import random
from time import monotonic as time_monotonic
from time import sleep

import pytest

from logpyle import (ETA, CallableLogQuantityAdapter, GCStats, IntervalTimer,
                     LogManager, LogQuantity, MultiLogQuantity, MultiPostLogQuantity,
                     PostLogQuantity, PushLogQuantity, StepToStepDuration,
                     TimestepCounter, TimestepDuration, WallTime)

# (name, value, unit, description, call_func)
test_logquantity_types = [
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


@pytest.fixture(params=test_logquantity_types)
def custom_logquantity(request):
    q_name, value, unit, description, call_func = request.param

    class TestLogQuantity(LogQuantity):
        def __init__(self, q_name, parameter) -> None:
            super().__init__(q_name, unit, description)
            setattr(self, q_name, parameter)
            self.func = call_func

        def __call__(self):
            # gather quantities
            value = getattr(self, self.name)

            # update value every time quantity is called
            new_val = self.func(value)
            setattr(self, q_name, new_val)

            return new_val

    obj = TestLogQuantity(q_name, value)

    yield obj


def test_logquantity(basic_logmgr, custom_logquantity):
    basic_logmgr.add_quantity(custom_logquantity)

    init_value = getattr(custom_logquantity, custom_logquantity.name)

    predicted_list = []
    calculated_list = []

    n = 20

    # generate prediction
    predicted_list.append(init_value)
    for _ in range(n):
        cur = predicted_list[-1]
        next = custom_logquantity.func(cur)
        predicted_list.append(next)

    for i in range(n):
        pre_val = getattr(custom_logquantity, custom_logquantity.name)
        assert pre_val == predicted_list[i]

        basic_logmgr.tick_before()
        # custom_logquantity should have been called
        middle_val = getattr(custom_logquantity, custom_logquantity.name)
        assert middle_val == predicted_list[i+1]
        basic_logmgr.tick_after()

        post_val = getattr(custom_logquantity, custom_logquantity.name)
        assert post_val == predicted_list[i+1]

        cur_vals = getattr(custom_logquantity, custom_logquantity.name)
        calculated_list.append(cur_vals)

    dataset = basic_logmgr.get_expr_dataset(custom_logquantity.name)
    stored_list = [tup[1] for tup in dataset[-1]]
    assert len(stored_list) == n

    print(calculated_list)
    print(stored_list)
    assert stored_list == calculated_list
    assert stored_list == predicted_list[1:]  # do not include initial vals


def test_logquantity_unimplemented(basic_logmgr: LogManager):
    quant = LogQuantity("q_one",)

    basic_logmgr.add_quantity(quant)

    with pytest.raises(NotImplementedError):
        basic_logmgr.tick_before()
        # do something
        basic_logmgr.tick_after()


@pytest.fixture(params=test_logquantity_types)
def custom_post_logquantity(request):
    q_name, value, unit, description, call_func = request.param

    class TestPostLogQuantity(PostLogQuantity):
        def __init__(self, q_name, parameter) -> None:
            super().__init__(q_name, unit, description)
            setattr(self, q_name, parameter)
            self.func = call_func

        def __call__(self):
            # gather quantities
            value = getattr(self, self.name)

            # update value every time quantity is called
            new_val = self.func(value)
            setattr(self, q_name, new_val)

            return new_val

    obj = TestPostLogQuantity(q_name, value)

    yield obj


def test_post_logquantity(basic_logmgr, custom_post_logquantity):
    basic_logmgr.add_quantity(custom_post_logquantity)

    init_value = getattr(custom_post_logquantity, custom_post_logquantity.name)

    predicted_list = []
    calculated_list = []

    n = 20

    # generate prediction
    predicted_list.append(init_value)
    for _ in range(n):
        cur = predicted_list[-1]
        next = custom_post_logquantity.func(cur)
        predicted_list.append(next)

    for i in range(n):
        pre_val = getattr(custom_post_logquantity, custom_post_logquantity.name)
        assert pre_val == predicted_list[i]

        basic_logmgr.tick_before()
        # custom_post_logquantity should not have been called
        middle_val = getattr(custom_post_logquantity, custom_post_logquantity.name)
        assert middle_val == predicted_list[i]
        basic_logmgr.tick_after()

        post_val = getattr(custom_post_logquantity, custom_post_logquantity.name)
        assert post_val == predicted_list[i+1]

        cur_vals = getattr(custom_post_logquantity, custom_post_logquantity.name)
        calculated_list.append(cur_vals)

    dataset = basic_logmgr.get_expr_dataset(custom_post_logquantity.name)
    stored_list = [tup[1] for tup in dataset[-1]]
    assert len(stored_list) == n

    print(calculated_list)
    print(stored_list)
    assert stored_list == calculated_list
    assert stored_list == predicted_list[1:]  # do not include initial vals


# (names, values, units, descriptions, call_func)
test_multi_log_quantity_types = [
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


@pytest.fixture(params=test_multi_log_quantity_types)
def custom_multi_log_quantity(request):
    names, values, units, descriptions, call_func = request.param

    class TestLogQuantity(MultiLogQuantity):
        def __init__(self, names, parameters) -> None:
            super().__init__(names, units, descriptions)
            for name, parameter in zip(names, parameters):
                setattr(self, name, parameter)
            self.func = call_func

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


def test_multi_log_quantity(basic_logmgr, custom_multi_log_quantity):
    basic_logmgr.add_quantity(custom_multi_log_quantity)

    init_values = []
    for name in custom_multi_log_quantity.names:
        init_values.append(getattr(custom_multi_log_quantity, name))

    predicted_list = []
    calculated_list = []

    n = 20

    # generate prediction
    predicted_list.append(init_values)
    for _ in range(n):
        cur = predicted_list[-1]
        next = custom_multi_log_quantity.func(*cur)
        predicted_list.append(next)

    for i in range(n):
        pre_vals = []
        for name in custom_multi_log_quantity.names:
            pre_vals.append(getattr(custom_multi_log_quantity, name))
        assert len(pre_vals) == len(init_values)
        assert pre_vals == predicted_list[i]

        basic_logmgr.tick_before()
        # custom_multi_log_quantity should have been called
        middle_vals = []
        for name in custom_multi_log_quantity.names:
            middle_vals.append(getattr(custom_multi_log_quantity, name))
        assert len(middle_vals) == len(init_values)
        assert middle_vals == predicted_list[i+1]
        basic_logmgr.tick_after()

        post_vals = []
        for name in custom_multi_log_quantity.names:
            post_vals.append(getattr(custom_multi_log_quantity, name))
        assert len(post_vals) == len(init_values)
        assert post_vals == predicted_list[i+1]

        cur_vals = []
        for name in custom_multi_log_quantity.names:
            cur_vals.append(getattr(custom_multi_log_quantity, name))
        calculated_list.append(cur_vals)

    stored_list = [[] for i in range(n)]
    for name in custom_multi_log_quantity.names:
        dataset = basic_logmgr.get_expr_dataset(name)
        tmp_stored_list = [tup[1] for tup in dataset[-1]]
        assert len(stored_list) == n
        for i, stored in enumerate(tmp_stored_list):
            stored_list[i].append(stored)

    print(calculated_list)
    print(stored_list)
    assert stored_list == calculated_list
    assert stored_list == predicted_list[1:]  # do not include initial vals


def test_multi_log_quantity_unimplemented(basic_logmgr: LogManager):
    quant = MultiLogQuantity(["q_one", "q_two"], ["1", "1"], ["q1", "q2"])

    basic_logmgr.add_quantity(quant)

    with pytest.raises(NotImplementedError):
        basic_logmgr.tick_before()
        # do something
        basic_logmgr.tick_after()


@pytest.fixture(params=test_multi_log_quantity_types)
def custom_multi_post_logquantity(request):
    names, values, units, descriptions, call_func = request.param

    class TestLogQuantity(MultiPostLogQuantity):
        def __init__(self, names, parameters) -> None:
            super().__init__(names, units, descriptions)
            for name, parameter in zip(names, parameters):
                setattr(self, name, parameter)
            self.func = call_func

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


def test_multi_post_logquantity(basic_logmgr, custom_multi_post_logquantity):
    basic_logmgr.add_quantity(custom_multi_post_logquantity)

    init_values = []
    for name in custom_multi_post_logquantity.names:
        init_values.append(getattr(custom_multi_post_logquantity, name))

    predicted_list = []
    calculated_list = []

    n = 20

    # generate prediction
    predicted_list.append(init_values)
    for _ in range(n):
        cur = predicted_list[-1]
        next = custom_multi_post_logquantity.func(*cur)
        predicted_list.append(next)

    for i in range(n):
        pre_vals = []
        for name in custom_multi_post_logquantity.names:
            pre_vals.append(getattr(custom_multi_post_logquantity, name))
        assert len(pre_vals) == len(init_values)
        assert pre_vals == predicted_list[i]

        basic_logmgr.tick_before()
        # custom_multi_post_logquantity should not have been called
        middle_vals = []
        for name in custom_multi_post_logquantity.names:
            middle_vals.append(getattr(custom_multi_post_logquantity, name))
        assert len(middle_vals) == len(init_values)
        assert middle_vals == predicted_list[i]
        basic_logmgr.tick_after()

        post_vals = []
        for name in custom_multi_post_logquantity.names:
            post_vals.append(getattr(custom_multi_post_logquantity, name))
        assert len(post_vals) == len(init_values)
        assert post_vals == predicted_list[i+1]

        cur_vals = []
        for name in custom_multi_post_logquantity.names:
            cur_vals.append(getattr(custom_multi_post_logquantity, name))
        calculated_list.append(cur_vals)

    stored_list = [[] for i in range(n)]
    for name in custom_multi_post_logquantity.names:
        dataset = basic_logmgr.get_expr_dataset(name)
        tmp_stored_list = [tup[1] for tup in dataset[-1]]
        assert len(stored_list) == n
        for i, stored in enumerate(tmp_stored_list):
            stored_list[i].append(stored)

    print(calculated_list)
    print(stored_list)
    assert stored_list == calculated_list
    assert stored_list == predicted_list[1:]  # do not include initial vals


def test_multi_post_logquantity_unimplemented(basic_logmgr: LogManager):
    quant = MultiPostLogQuantity(["q_one", "q_two"], ["1", "1"], ["q1", "q2"])

    basic_logmgr.add_quantity(quant)

    with pytest.raises(NotImplementedError):
        basic_logmgr.tick_before()
        # do something
        basic_logmgr.tick_after()


def test_accurate_timestepcounter_quantity(basic_logmgr: LogManager):
    test_timer = TimestepCounter("t_step_count")
    basic_logmgr.add_quantity(test_timer)

    n1 = 200
    n2 = 120

    for _ in range(n1):
        basic_logmgr.tick_before()
        # do something ...
        basic_logmgr.tick_after()
    assert basic_logmgr.last_values["t_step_count"] == n1 - 1

    for _ in range(n2):
        basic_logmgr.tick_before()
        # do something ...
        basic_logmgr.tick_after()
    assert basic_logmgr.last_values["t_step_count"] == n1 + n2 - 1


test_steptostep_and_timestepduration_quantity_data = [
    (TimestepDuration("t_slp")),
    (StepToStepDuration("t_slp")),
]


@pytest.mark.parametrize("test_timer",
                         test_steptostep_and_timestepduration_quantity_data)
def test_steptostep_and_timestepduration_quantity(
        test_timer: any,
        basic_logmgr: LogManager
        ):
    tol = 0.005
    min_time = 0.02

    basic_logmgr.add_quantity(test_timer)

    n = 20

    sleep_times = [random.random() / 30 + min_time for i in range(n)]

    for i in range(n):
        if isinstance(test_timer, StepToStepDuration):
            sleep(sleep_times[i])

        basic_logmgr.tick_before()
        if isinstance(test_timer, TimestepDuration):
            sleep(sleep_times[i])
        basic_logmgr.tick_after()

    # first value is not defined for StepToStep, so we drop it
    if isinstance(test_timer, StepToStepDuration):
        del sleep_times[0]

    actual_times = [tup[1] for tup in basic_logmgr.get_expr_dataset("t_slp")[2]]
    print(actual_times, sleep_times)
    # assert that these quantities only differ by a max of tol
    # defined above
    for (predicted, actual) in zip(sleep_times, actual_times):
        assert abs(actual - predicted) < tol


def test_accurate_walltime_quantity(basic_logmgr: LogManager):
    tol = 0.1
    min_time = 0.02

    n = 20

    test_timer = WallTime("t_total")
    start_time = time_monotonic()

    basic_logmgr.add_quantity(test_timer)
    for _ in range(n):
        sleep_before_time = random.random() / 30 + min_time
        sleep_during_time = random.random() / 30 + min_time

        sleep(sleep_before_time)

        basic_logmgr.tick_before()
        sleep(sleep_during_time)
        basic_logmgr.tick_after()

        now = time_monotonic()
        total_time = now - start_time
        actual_time = basic_logmgr.get_expr_dataset("t_total")[-1][-1][-1]
        print(total_time, actual_time)
        # assert that these quantities only differ by a max of tol
        # defined above
        assert abs(total_time - actual_time) < tol


def test_basic_push_log_quantity(basic_logmgr: LogManager):
    push_quantity = PushLogQuantity("pusher")
    basic_logmgr.add_quantity(push_quantity)

    for i in range(20):
        push_quantity.push_value(i)
        basic_logmgr.tick_before()
        # do something ...
        basic_logmgr.tick_after()
        print(basic_logmgr.get_expr_dataset("pusher"))
        assert basic_logmgr.get_expr_dataset("pusher")[-1][-1][-1] == i


def test_double_push_push_log_quantity(basic_logmgr: LogManager):
    push_quantity = PushLogQuantity("pusher")
    basic_logmgr.add_quantity(push_quantity)

    first_val = 25
    second_val = 36

    push_quantity.push_value(first_val)
    with pytest.raises(RuntimeError):
        push_quantity.push_value(second_val)


def test_callable_logquantity(basic_logmgr: LogManager):
    global counter
    counter = 0

    def called_func() -> float:
        global counter
        counter += 1
        return random.random()

    callable = CallableLogQuantityAdapter(called_func, "caller")
    basic_logmgr.add_quantity(callable)

    n = 50
    for _ in range(n):
        basic_logmgr.tick_before()
        # do something ...
        basic_logmgr.tick_after()

    print(counter)
    assert counter == n


def test_update_constants(basic_logmgr: LogManager):
    basic_logmgr.set_constant("value", 27)

    assert basic_logmgr.constants["value"] == 27

    basic_logmgr.tick_before()
    # do something ...
    assert basic_logmgr.constants["value"] == 27
    basic_logmgr.tick_after()

    assert basic_logmgr.constants["value"] == 27

    basic_logmgr.set_constant("value", 81)

    assert basic_logmgr.constants["value"] == 81

    basic_logmgr.tick_before()
    # do something ...
    assert basic_logmgr.constants["value"] == 81
    basic_logmgr.tick_after()

    assert basic_logmgr.constants["value"] == 81


def test_multi_log_quantity_call_not_implemented(basic_logmgr: LogManager):
    multi_log = MultiLogQuantity(["q_one", "q_two"])
    basic_logmgr.add_quantity(multi_log)
    with pytest.raises(NotImplementedError):
        multi_log()


def test_interval_timer_subtimer(basic_logmgr: LogManager):
    tol = 0.1
    timer = IntervalTimer("timer")
    basic_logmgr.add_quantity(timer)

    expected_timer_list = []

    n = 20
    for _ in range(n):
        good_sleep_time = (random.random()/10 + 0.1)
        bad_sleep_time = (random.random()/10 + 0.1)
        expected_timer_list.append(good_sleep_time)
        sub_timer = timer.get_sub_timer()

        basic_logmgr.tick_before()
        sub_timer.start()
        sleep(good_sleep_time)
        sub_timer.stop()
        sub_timer.submit()
        # do something
        sleep(bad_sleep_time)
        sleep(bad_sleep_time)
        basic_logmgr.tick_after()

    val = basic_logmgr.get_expr_dataset("timer")[-1]
    val_list = [data[1] for data in val]
    print(val_list)
    print(expected_timer_list)

    # enforce equality of durations
    for tup in zip(val_list, expected_timer_list):
        assert abs(tup[0] - tup[1]) < tol


def test_interval_timer_subtimer_blocking(basic_logmgr: LogManager):
    tol = 0.1
    timer = IntervalTimer("timer")
    basic_logmgr.add_quantity(timer)

    expected_timer_list = []

    n = 20
    for _ in range(n):
        good_sleep_time = (random.random()/10 + 0.1)
        bad_sleep_time = (random.random()/10 + 0.1)
        expected_timer_list.append(good_sleep_time)
        sub_timer = timer.get_sub_timer()

        basic_logmgr.tick_before()
        with sub_timer:
            sleep(good_sleep_time)
        # do something
        sleep(bad_sleep_time)
        sleep(bad_sleep_time)
        basic_logmgr.tick_after()

    val = basic_logmgr.get_expr_dataset("timer")[-1]
    val_list = [data[1] for data in val]
    print(val_list)
    print(expected_timer_list)

    # enforce equality of durations
    for tup in zip(val_list, expected_timer_list):
        assert abs(tup[0] - tup[1]) < tol


def test_accurate_eta_quantity(basic_logmgr: LogManager):
    # should begin calculation and ensure that the true time is
    # within a tolerance of the estimated time
    tol = 0.25

    n = 30

    test_timer = ETA(n-1, "t_fin")
    basic_logmgr.add_quantity(test_timer)

    sleep_time = 0.02

    predicted_time = n * sleep_time

    for i in range(n):
        basic_logmgr.tick_before()
        sleep(sleep_time)
        basic_logmgr.tick_after()

        eta_time = basic_logmgr.get_expr_dataset("t_fin")[-1][-1][-1]
        predicted_time -= sleep_time

        if i > 0:
            # ETA isn't available on step 0.
            assert abs(predicted_time-eta_time) < tol
        print(i, f"{eta_time=}", f"{predicted_time=}",
              abs(eta_time - predicted_time))

    assert 0 <= eta_time < 1e-12
    assert abs(predicted_time) < 1e-12


def test_gc_stats(basic_logmgr: LogManager):
    # will check if the example code breaks from using GCStats
    # should expand on later
    # currently ensures that after some time, GC from generation 1
    # eventually goes into generation 2
    gc_stats = GCStats()
    basic_logmgr.add_quantity(gc_stats)

    outer_list = []

    last = None
    memory_has_changed_generation = False

    for istep in range(1000):
        basic_logmgr.tick_before()

        soon_tobe_lost_ref = ["garb1", "garb2", "garb3"] * istep
        outer_list.append(([soon_tobe_lost_ref]))

        basic_logmgr.tick_after()

        sleep(0.02)

        cur = gc_stats()
        # [enabled, # in generation1,  # in generation2, # in generation3,
        #  gen1 collections, gen1 collected, gen1 uncollected,
        #  gen2 collections, gen2 collected, gen2 uncollected,
        #  gen3 collections, gen3 collected, gen3 uncollected]
        print(cur)
        if last is not None and cur[2] > last[2]:
            memory_has_changed_generation = True
        last = cur

    assert memory_has_changed_generation
