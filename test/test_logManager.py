import logging
import pytest
from random import uniform
from time import (sleep,monotonic as time_monotonic)
from warnings import warn


from logpyle import (GCStats, IntervalTimer, LogManager, LogQuantity,
                     add_general_quantities, add_run_info,
                     add_simulation_quantities, set_dt)


# Notes to self
# 
# 1) warnings seem to only work with the warnings module and nothing gets logged with the logging module
# 2) 
#
#
#

def test_example():
	assert True

def test_start_time_has_past():
	logmgr = LogManager("log.sqlite", "wo")
	assert logmgr.start_time <= time_monotonic()
	logmgr.close()

def test_empty_on_init():
	logmgr = LogManager("log.sqlite", "wo")

	# ensure that there are no initial watches
	assert len(logmgr.watches) == 0 

	logmgr.close()
    
def test_basic_warning():
	with pytest.warns(UserWarning):
		warn("Oof. Something went awry.", UserWarning)


def test_logging_warnings_from_warnings_module():
	logmgr = LogManager("log.sqlite", "wo")

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

	assert True


def test_accurate_interval_timer():
	# todo
	pass


def test_accurate_general_quantities():
	# todo
	pass


def test_accurate_simmulation_quantities():
	# todo
	pass