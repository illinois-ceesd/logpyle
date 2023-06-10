import logging
import pytest
import random
import mpi4py 
from random import uniform
from time import (sleep,monotonic as time_monotonic)
from warnings import warn


from logpyle import *


# Notes to self
# 
# 1) 
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
	assert logmgr.logging_data[0].category == 'WARNING'
	assert logmgr.logging_data[0].tick_count == 0
	
	second_warning_message = "Not a warning: Second warning message!!!"
	
	logmgr.tick_before()
	logger.warning(second_warning_message)
	logmgr.tick_after()

	# ensure that the warning was caught properly
	print(logmgr.logging_data[1])
	assert logmgr.logging_data[1].message == second_warning_message
	assert logmgr.logging_data[1].category == 'WARNING'
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
		sleepTime = random.random()/30 + minTime
		logmgr.tick_after()

		sleep(sleepTime)

		logmgr.tick_before()
		print(sleepTime,test_timer()) # assert that these quantities only differ by a max of tol defined above
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
		sleepTime = random.random()/30 + minTime

		logmgr.tick_before()
		sleep(sleepTime)
		logmgr.tick_after()

		actual_time = logmgr.get_expr_dataset("t_slp")[2][-1][1]
		print(sleepTime,actual_time) # assert that these quantities only differ by a max of tol defined above
		assert abs(actual_time - sleepTime) < tol

	logmgr.close()
	pass


def test_accurate_Eta_quantity():
	# todo
	# should begin calculation and ensure that the true time is within a tolerance of the estimated time
	logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")


	logmgr.close()
	pass


def test_accurate_WallTime_quantity():
	# todo
	logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")


	logmgr.close()
	pass


def test_accurate_InitTime_quantity():
	# todo
	logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")


	logmgr.close()
	pass


def test_general_quantities():
	# todo
	logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")


	logmgr.close()
	pass


def test_simulation_quantities():
	# todo
	logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")


	logmgr.close()
	pass


def test_add_run_info():
	# todo
	logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")


	logmgr.close()
	pass


def test_accurate_BLANK_quantity():
	# todo
	logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")


	logmgr.close()
	pass


def test_unimplemented_logging_quantity():
	# todo
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
	# todo
	# will check if the example code breaks from using GCStats
	# should expand on later
	logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")


	logmgr.close()
	pass


def test_set_dt():
	# todo
	# Should verify that the dt is changed and is applied to dt consuming quantities after changing
	logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")


	logmgr.close()
	pass


def test_MultiPostLogQuantity():
	# todo
	logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")


	logmgr.close()
	pass


def test_add_watches():
	# todo
	# test adding a few watches
	logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")


	logmgr.close()
	pass


def test_nameless_LogManager():
	# todo
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
	# todo
	logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")


	logmgr.close()
	pass


def test_accurate_BLANK_quantity():
	# todo
	logmgr = LogManager("THIS_LOG_SHOULD_BE_DELETED", "wo")


	logmgr.close()
	pass











# -------------------- Time Intensive Tests --------------------



def test_MemoryHwm_quantity():
	# todo
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
	logmgr.add_watches(["step.max", "t_sim.max", "t_step.max",
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

		if istep == 50:
			print("FYI: Setting watch interval to 5 seconds.")
			logmgr.set_watch_interval(5)

		if istep == 150:
			print("FYI: Setting watch interval back to 1 second.")
			logmgr.set_watch_interval(1)

		logmgr.tick_after()



	logmgr.close()
	pass

