#!/usr/bin/env python3

from time import sleep
from random import uniform
from logpyle import (LogManager, add_general_quantities,
        add_simulation_quantities, add_run_info, IntervalTimer,
        LogQuantity, set_dt)

from warnings import warn
from mpi4py import MPI


class Fifteen(LogQuantity):
    @property
    def default_aggregator(self):
        return min

    def __call__(self):
        return 15


def main():
    logmgr = LogManager("mpi-log.sqlite", "wu", MPI.COMM_WORLD)

    # Set a run property
    logmgr.set_constant("myconst", uniform(0, 1))

    size = MPI.COMM_WORLD.Get_size()
    rank = MPI.COMM_WORLD.Get_rank()

    print(f"Rank {rank} of {size} ranks.")

    # Generic run metadata, such as command line, host, and time
    add_run_info(logmgr)

    # Time step duration, wall time, ...
    add_general_quantities(logmgr)

    # Simulation time, time step
    add_simulation_quantities(logmgr)

    # Additional quantities to log
    vis_timer = IntervalTimer("t_vis", "Time spent visualizing")
    logmgr.add_quantity(vis_timer)
    logmgr.add_quantity(Fifteen("fifteen"))

    # Watches are printed periodically during execution
    logmgr.add_watches(
        ["step.max", "t_sim.max", "t_step.max", "fifteen", "t_vis.max"])

    for istep in range(200):
        logmgr.tick_before()

        dt = uniform(0.01, 0.1)
        set_dt(logmgr, dt)
        sleep(dt)

        # Illustrate custom timers
        if istep % 10 == 0:
            with vis_timer.start_sub_timer():
                sleep(0.05)

        # Illustrate warnings capture
        if uniform(0, 1) < 0.05:
            warn("Oof. Something went awry.")

        logmgr.tick_after()

    logmgr.close()


if __name__ == "__main__":
    main()
