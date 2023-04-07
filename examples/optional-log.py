#!/usr/bin/env python3

from contextlib import nullcontext
from random import uniform
from time import sleep
from typing import Union

from logpyle import (IntervalTimer, LogManager, _SubTimer,
                     add_general_quantities, add_run_info,
                     add_simulation_quantities, set_dt)


def main(use_logpyle: bool) -> None:
    if use_logpyle:
        logmgr = LogManager("optional-log.sqlite", "w")
    else:
        logmgr = None

    # See examples/log.py for details about these
    if logmgr:
        add_run_info(logmgr)
        add_general_quantities(logmgr)
        add_simulation_quantities(logmgr)

    # Add a timer quantity and retrieve a sub-timer for later use
    if logmgr:
        vis_timer = IntervalTimer("t_vis", "Time spent visualizing")
        logmgr.add_quantity(vis_timer)
        time_vis: Union[_SubTimer, nullcontext[None]] = vis_timer.get_sub_timer()
    else:
        time_vis = nullcontext()

    if logmgr:
        logmgr.add_watches(["step.max", "t_sim.max", "t_step.max", "t_vis"])

    for istep in range(200):
        if logmgr:
            logmgr.tick_before()

        dt = uniform(0.01, 0.1)
        if logmgr:
            set_dt(logmgr, dt)

        sleep(dt)

        if istep % 10 == 0:
            # Use the sub-timer
            with time_vis:
                sleep(0.05)

        if logmgr:
            logmgr.tick_after()

    if logmgr:
        logmgr.close()


if __name__ == "__main__":
    main(use_logpyle=True)
