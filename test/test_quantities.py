import pytest

from testlib import basicLogmgr
from logpyle import (
    LogManager,
    LogQuantity,
    PostLogQuantity,
    MultiLogQuantity,
    MultiPostLogQuantity,
)

# notes to self
# 1) might be benificial to warn user if they attempt to name a quantity
#       begining with a number. SQLite doesnt like that very much
# 2) Certain quantities modify their values on call as opposed to on tick.
#       (PushLogQuantity, IntervalTimer,
#       TimestepCounter, TimestepDuration, InitTime, ETA)
# 3) TimestepCounter states that it counts the number of LogManager ticks, but
#       it actually counts the number of times tick_before was called
# 4) It seems as if None is always casted to be nonexistent in the database


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
        # blueprint
        # (
        #     "Quantity_name",
        #     1,
        #     "1",
        #     "Q init to 1",
        #     lambda x: x+1
        #  ),
        # currently does not handle values of None cleanly
        # (
        #     "Quantity_name",
        #     None,
        #     "1",
        #     "Q init to 1",
        #     lambda x: x
        #  ),
        ]


@pytest.fixture(params=test_LogQuantity_types)
def customLogQuantity(request):
    name, value, unit, description, callFunc = request.param

    class TestLogQuantity(LogQuantity):
        def __init__(self, name, parameter) -> None:
            super().__init__(name, unit, description)
            setattr(self, name, parameter)
            self.func = callFunc

        def __call__(self):
            # gather quantities
            value = getattr(self, self.name)

            # update value every time quantity is called
            new_val = self.func(value)
            setattr(self, name, new_val)

            return new_val

    obj = TestLogQuantity(name, value)

    yield obj


def test_LogQuantity(basicLogmgr, customLogQuantity):
    # TODO
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
        # assert
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
    name, value, unit, description, callFunc = request.param

    class TestPostLogQuantity(PostLogQuantity):
        def __init__(self, name, parameter) -> None:
            super().__init__(name, unit, description)
            setattr(self, name, parameter)
            self.func = callFunc

        def __call__(self):
            # gather quantities
            value = getattr(self, self.name)

            # update value every time quantity is called
            new_val = self.func(value)
            setattr(self, name, new_val)

            return new_val

    obj = TestPostLogQuantity(name, value)

    yield obj


def test_PostLogQuantity(basicLogmgr, customPostLogQuantity):
    # TODO
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
        # assert
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
        # blueprint
        # (
        #     ["Quantity_1", "Quantity_2"],
        #     [1, 2],
        #     ["1", "1"],
        #     ["Q init to 1", "Q init to 2"],
        #     lambda x, y: [x+1, y+1]
        #  ),
        # currently does not handle values of None cleanly
        # (
        #     ["Quantity_1", "Quantity_2"],
        #     [None, 2],
        #     ["1", "1"],
        #     ["Q init to 1", "Q init to 2"],
        #     lambda x, y: [x, y+1]
        #  ),
        # (
        #     ["Quantity_1", "Quantity_2"],
        #     [1, None],
        #     ["1", "1"],
        #     ["Q init to 1", "Q init to 2"],
        #     lambda x, y: [x+1, y]
        #  ),
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
    # TODO
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
        # assert
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
    # TODO
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
        # assert
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
