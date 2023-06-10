import logging
import pytest
import random
from random import uniform
from time import (sleep,monotonic as time_monotonic)
from warnings import warn


from logpyle import *


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


def test_logging_warnings_from_logging_module():
	logmgr = LogManager("log.sqlite", "wo")
	
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
	logmgr = LogManager("log.sqlite", "wo")

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
	logmgr = LogManager("log.sqlite", "wo")

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
	logmgr = LogManager("log.sqlite", "wo")
	
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


def test_accurate_BLANK_quantity():
	# todo
	logmgr = LogManager("log.sqlite", "wo")


	logmgr.close()
	pass

