import os
import sys
from time import sleep

import pytest

from logpyle import LogManager, LogQuantity

# {{{ mpi test infrastructure


def run_test_with_mpi(num_ranks, f, *args, extra_env_vars=None):
    pytest.importorskip("mpi4py")

    if extra_env_vars is None:
        extra_env_vars = {}

    from base64 import b64encode
    from pickle import dumps
    from subprocess import check_call

    env_vars = {
            "RUN_WITHIN_MPI": "1",
            "INVOCATION_INFO": b64encode(dumps((f, args))).decode(),
            }
    env_vars.update(extra_env_vars)

    # NOTE: CI uses OpenMPI; -x to pass env vars. MPICH uses -env
    check_call([
        "mpiexec", "-np", str(num_ranks),
        "--oversubscribe",
        ] + [
            item
            for env_name, env_val in env_vars.items()
            for item in ["-x", f"{env_name}={env_val}"]
        ] + [sys.executable, "-m", "mpi4py", __file__])


def run_test_with_mpi_inner():
    from base64 import b64decode
    from pickle import loads
    f, args = loads(b64decode(os.environ["INVOCATION_INFO"].encode()))

    f(*args)


# }}}

def setup_manager() -> LogManager:
    from mpi4py import MPI  # pylint: disable=import-error

    comm = MPI.COMM_WORLD

    filename = "THIS_LOG_SHOULD_BE_DELETED.sqlite"
    logmgr = LogManager(filename, "wu", comm)
    return logmgr, comm


def teardown_manager(logmgr: LogManager):
    logmgr.close()


def cleanup_files():
    sleep(5)

    def is_unique_filename(str: str):
        return str.startswith("THIS_LOG_SHOULD_BE_DELETED-")

    files = [f for f in os.listdir() if is_unique_filename(f)]
    for f in files:
        os.remove(f)


@pytest.mark.parametrize("execution_number", range(1))
def test_distributed_execution_basic(execution_number):
    run_test_with_mpi(2, _do_test_distributed_execution_basic)

    cleanup_files()


def _do_test_distributed_execution_basic():
    logmgr, comm = setup_manager()

    rank = comm.Get_rank()
    size = comm.Get_size()

    print("Rank " + str(rank) + " of " + str(size))

    print(str(rank), str(logmgr.rank))
    assert rank == logmgr.rank
    assert logmgr.is_parallel is True

    teardown_manager(logmgr)


@pytest.mark.parametrize("execution_number", range(1))
def test_distributed_execution_add_watches(execution_number):
    run_test_with_mpi(2, _do_test_distributed_execution_basic)

    cleanup_files()


def _do_test_distributed_execution_add_watches():
    logmgr, comm = setup_manager()

    rank = comm.Get_rank()
    size = comm.Get_size()

    print("Rank " + str(rank) + " of " + str(size))

    class Fifteen(LogQuantity):
        def __call__(self) -> int:
            return 15

    class FifteenStr(LogQuantity):
        def __call__(self) -> str:
            return "15.0"

    logmgr.add_quantity(Fifteen("name1"))
    logmgr.add_quantity(Fifteen("name2"))
    logmgr.add_quantity(FifteenStr("tup_name1"))

    watch_list = ["name1", ("tup_name1", "str"), "name2"]

    logmgr.add_watches(watch_list)

    logmgr.tick_before()
    # do something ...
    logmgr.tick_before()
    logmgr.save()

    # check that all watches are present
    actual_watches = [watch.expr for watch in logmgr.watches]
    expected = ["name1", "tup_name1", "name2"]
    actual_watches.sort()
    expected.sort()
    print(actual_watches, expected)
    assert actual_watches == expected

    teardown_manager(logmgr)


if __name__ == "__main__":
    if "RUN_WITHIN_MPI" in os.environ:
        run_test_with_mpi_inner()
    elif len(sys.argv) > 1:
        exec(sys.argv[1])
    else:
        from pytest import main
        main([__file__])
