#!/usr/bin/env python3

import logging
from random import uniform
from time import sleep
from warnings import warn

from logpyle import (GCStats, IntervalTimer, LogManager, LogQuantity,
                     add_general_quantities, add_run_info,
                     add_simulation_quantities, set_dt)


class Fifteen(LogQuantity):
    def __call__(self) -> int:
        return 15


logger = logging.getLogger(__name__)


def main() -> None:
    logmgr = LogManager("log.sqlite", "wo")

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
    logmgr.add_quantity(Fifteen("fifteen"))
    logmgr.add_quantity(GCStats())

    # Watches are printed periodically during execution
    logmgr.add_watches(["step.max", "t_sim.max", "t_step.max", "fifteen",
                        "t_vis", "t_log", "memory_usage_hwm"])

    for istep in range(200):
        logmgr.tick_before()

        dt = uniform(0.01, 0.05)
        set_dt(logmgr, dt)
        sleep(dt)

        # Illustrate custom timers
        if istep % 10 == 0:
            with vis_timer.start_sub_timer():
                sleep(0.05)

        # Illustrate warnings capture
        if uniform(0, 1) < 0.05:
            warn("Oof. Something went awry.")

        if istep == 50:
            logger.warning("test logging")
            print("FYI: Setting watch interval to 5 seconds.")
            logmgr.set_watch_interval(5)

        if istep == 150:
            logger.error("another logging test")
            print("FYI: Setting watch interval back to 1 second.")
            logmgr.set_watch_interval(1)

        logmgr.tick_after()

    logmgr.close()


if __name__ == "__main__":
    main()
