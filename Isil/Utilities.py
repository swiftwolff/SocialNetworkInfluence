

def elapsed_time(start_time, end_time):
	
	elapsed_time_seconds = "Elapsed time was %g seconds" % (end_time - start_time)
	elapsed_time_minutes = "Elapsed time was %g minutes" % int((end_time - start_time)/60)
	elapsed_time_hours = "Elapsed time was %g hours" % int((end_time - start_time)/3600)
	
	return elapsed_time_seconds, elapsed_time_minutes, elapsed_time_hours
