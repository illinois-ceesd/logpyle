#!/usr/bin/env python3

from time import sleep
from typing import Any
from random import uniform
from logpyle import (LogManager, add_general_quantities,
        add_simulation_quantities, add_run_info, IntervalTimer,
        LogQuantity, set_dt)

from warnings import warn
from mpi4py import MPI


class PushLogQuantity(LogQuantity):
    """Logging support for arbitrary user quantities."""

    def __init__(self, name, value=None, unit=None,
          description=None) -> None:
        LogQuantity.__init__(self, name=name, unit=unit,
                             description=description)
        self._quantity_value = value

    def __call__(self) -> float:
        """Return the actual logged quantity."""
        val = self._quantity_value
        self._quantity_value = None
        return val

    def set_quantity_value(self, value: Any) -> None:
        """Set the value of the logged quantity."""
        self._quantity_value = value


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
    logmgr.add_quantity(PushLogQuantity("q1"))

    # Watches are printed periodically during execution
    logmgr.add_watches([("step.max", "step={value} "),
                        ("t_step.min", "\nt_step({value:g},"),
                        ("t_step.max", " {value:g})\n"),
                        ("q1.max", " UserQ1:({value:g}), "),
                        "t_sim.max", "fifteen", "t_vis.max"])

    for istep in range(200):
        logmgr.tick_before()
        logmgr.set_quantity_value("q1", 2*istep)

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
