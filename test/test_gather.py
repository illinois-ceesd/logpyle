import os
import subprocess


def test_auto_gather_single():
    # run example
    os.system("python ../examples/log.py")
    assert os.path.exists("log.sqlite"), "The logging file was not generated."

    # check schema

    # ensure logging table exists
    result = subprocess.run("runalyzer log.sqlite -c 'db.print_cursor(\
        db.q(\"select * from logging\"))'",
        shell=True, capture_output=True, text=True).stdout.strip()
    print("Logging data:")
    print(result)
    # ensure warnings table exists
    result = subprocess.run("runalyzer log.sqlite -c 'db.print_cursor(\
            db.q(\"select * from warnings\"))'",
            shell=True, capture_output=True, text=True).stdout.strip()
    print("Warnings data:")
    print(result)
    # ensure quantity in runs table exists
    result = subprocess.run("runalyzer log.sqlite -c 'db.print_cursor(\
            db.q(\"select $t_log.max\"))'",
            shell=True, capture_output=True, text=True).stdout.strip()
    print("Quantity data:")
    print(result)

    # check constant
    result = subprocess.run("runalyzer log.sqlite -c 'db.print_cursor(\
            db.q(\"select $fifteen\"))'",
            shell=True, capture_output=True, text=True).stdout.strip()
    print("Constant data:")
    print(result)
    constant_entries = result.splitlines()[3:]
    for line in constant_entries:
        assert float(line.strip()) == 15

    # teardown test
    os.remove("log.sqlite")


def test_auto_gather_multi():
    # run example
    def is_unique_filename(str: str):
        return str.startswith("mpi-log-rank")

    n = 2

    log_files = [f for f in os.listdir() if is_unique_filename(f)]
    assert len(log_files) == 0  # no initial mpi-log files

    os.system(f"mpiexec -n {n} ../examples/log-mpi.py")

    log_files = [f for f in os.listdir() if is_unique_filename(f)]
    assert len(log_files) == n, "The logging files were not generated."

    # check schema

    # ensure logging table exists
    result = subprocess.run("runalyzer mpi-log*.sqlite -c 'db.print_cursor(\
        db.q(\"select * from logging\"))'",
        shell=True, capture_output=True, text=True).stdout.strip()
    print("Logging data:")
    print(result)
    # ensure warnings table exists
    result = subprocess.run("runalyzer mpi-log*.sqlite -c 'db.print_cursor(\
            db.q(\"select * from warnings\"))'",
            shell=True, capture_output=True, text=True).stdout.strip()
    print("Warnings data:")
    print(result)
    # ensure quantity in runs table exists
    result = subprocess.run("runalyzer mpi-log*.sqlite -c 'db.print_cursor(\
            db.q(\"select $t_log.max\"))'",
            shell=True, capture_output=True, text=True).stdout.strip()
    print("Quantity data:")
    print(result)

    # check constant
    result = subprocess.run("runalyzer mpi-log*.sqlite -c 'db.print_cursor(\
            db.q(\"select $fifteen\"))'",
            shell=True, capture_output=True, text=True).stdout.strip()
    print("Constant data:")
    print(result)
    constant_entries = result.splitlines()[3:]
    for line in constant_entries:
        assert float(line.strip()) == 15

    # check aggregate
    max_t_log = subprocess.run("runalyzer mpi-log*.sqlite -c 'db.print_cursor(\
            db.q(\"select $t_log.max\"))'",
            shell=True, capture_output=True, text=True).stdout.strip()
    min_t_log = subprocess.run("runalyzer mpi-log*.sqlite -c 'db.print_cursor(\
            db.q(\"select $t_log.min\"))'",
            shell=True, capture_output=True, text=True).stdout.strip()
    print("Quantity data:")
    max_min_t_log = zip(max_t_log.splitlines()[3:], min_t_log.splitlines()[3:])
    for max, min in max_min_t_log:
        print(max.strip(), min.strip())
        assert float(max.strip()) > float(min.strip())

    # teardown test
    log_files = [f for f in os.listdir() if is_unique_filename(f)]
    for f in log_files:
        os.remove(f)


def test_manual_gather_single():
    # run example
    os.system("python ../examples/log.py")
    assert os.path.exists("log.sqlite"), "The logging file was not generated."

    # gather example sqlite
    os.system("runalyzer-gather summary.sqlite log.sqlite")
    assert os.path.exists("summary.sqlite"), "The logging file was not gathered."

    # check schema

    # ensure logging table exists
    result = subprocess.run("runalyzer summary.sqlite -c 'db.print_cursor(\
        db.q(\"select * from logging\"))'",
        shell=True, capture_output=True, text=True).stdout.strip()
    print("Logging data:")
    print(result)
    # ensure warnings table exists
    result = subprocess.run("runalyzer summary.sqlite -c 'db.print_cursor(\
            db.q(\"select * from warnings\"))'",
            shell=True, capture_output=True, text=True).stdout.strip()
    print("Warnings data:")
    print(result)
    # ensure quantity in runs table exists
    result = subprocess.run("runalyzer summary.sqlite -c 'db.print_cursor(\
            db.q(\"select $t_log.max\"))'",
            shell=True, capture_output=True, text=True).stdout.strip()
    print("Quantity data:")
    print(result)

    # check constant
    result = subprocess.run("runalyzer summary.sqlite -c 'db.print_cursor(\
            db.q(\"select $fifteen\"))'",
            shell=True, capture_output=True, text=True).stdout.strip()
    print("Constant data:")
    print(result)
    constant_entries = result.splitlines()[2:]
    for line in constant_entries:
        assert float(line.strip()) == 15

    # teardown test
    os.remove("log.sqlite")
    os.remove("summary.sqlite")


def test_manual_gather_multi():
    # run example
    def is_unique_filename(str: str):
        return str.startswith("mpi-log-rank")

    n = 2

    log_files = [f for f in os.listdir() if is_unique_filename(f)]
    assert len(log_files) == 0  # no initial mpi-log files

    os.system(f"mpiexec -n {n} ../examples/log-mpi.py")

    log_files = [f for f in os.listdir() if is_unique_filename(f)]
    assert len(log_files) == n, "The logging files were not generated."

    # gather example sqlite
    os.system("runalyzer-gather summary.sqlite mpi-log*.sqlite")
    assert os.path.exists("summary.sqlite"), "The logging file was not gathered."

    # check schema

    # ensure logging table exists
    result = subprocess.run("runalyzer summary.sqlite -c 'db.print_cursor(\
        db.q(\"select * from logging\"))'",
        shell=True, capture_output=True, text=True).stdout.strip()
    print("Logging data:")
    print(result)
    # ensure warnings table exists
    result = subprocess.run("runalyzer summary.sqlite -c 'db.print_cursor(\
            db.q(\"select * from warnings\"))'",
            shell=True, capture_output=True, text=True).stdout.strip()
    print("Warnings data:")
    print(result)
    # ensure quantity in runs table exists
    result = subprocess.run("runalyzer summary.sqlite -c 'db.print_cursor(\
            db.q(\"select $t_log.max\"))'",
            shell=True, capture_output=True, text=True).stdout.strip()
    print("Quantity data:")
    print(result)

    # check constant
    result = subprocess.run("runalyzer summary.sqlite -c 'db.print_cursor(\
            db.q(\"select $fifteen\"))'",
            shell=True, capture_output=True, text=True).stdout.strip()
    print("Constant data:")
    print(result)
    constant_entries = result.splitlines()[2:]
    for line in constant_entries:
        assert float(line.strip()) == 15

    # check aggregate
    max_t_log = subprocess.run("runalyzer summary.sqlite -c 'db.print_cursor(\
            db.q(\"select $t_log.max\"))'",
            shell=True, capture_output=True, text=True).stdout.strip()
    min_t_log = subprocess.run("runalyzer summary.sqlite -c 'db.print_cursor(\
            db.q(\"select $t_log.min\"))'",
            shell=True, capture_output=True, text=True).stdout.strip()
    print("Quantity data:")
    max_min_t_log = zip(max_t_log.splitlines()[2:], min_t_log.splitlines()[2:])
    for max, min in max_min_t_log:
        print(max.strip(), min.strip())
        assert float(max.strip()) > float(min.strip())

    # teardown test
    log_files = [f for f in os.listdir() if is_unique_filename(f)]
    for f in log_files:
        os.remove(f)
    os.remove("summary.sqlite")
