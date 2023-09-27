from datetime import datetime
import csv
import os
from pyspark.sql import functions as F


# This is needed to have a verbose option. If the VERBOSE environment variable is set to True, the verbose option is activated, Default is False.
verbose = bool(os.getenv('VERBOSE', 'False'))
v_print = print if verbose else lambda *a, **k: None


def check_numeric(df, column:str , check:str, boundary = None, boundary2 = None, gravity = 'Warning'):
	checks = ['greater', 'less', 'equal', 'greater_equal', 'less_equal', 'not_equal', 'between', 'not_between']
	if check not in checks:
		raise ValueError('Error: Invalid check type; expected one of: %s' % checks)
	if ( isinstance(boundary, (int, float, complex)) == False or isinstance(boundary2, (int, float, complex)) == False):
		raise ValueError('Error: Boundary must be a number')
	# need to check if the column is numeric
	v_print('Warning: Verification of column type is not implemented yet')

	if check == 'greater':
			result = df.filter(df[column] >= boundary)
			if boundary2 != None : v_print('Warning: Boundary2 is not used in this check')
	elif check == 'less':
			result = df.filter(df[column] <= boundary)
			if boundary2 != None : v_print('Warning: Boundary2 is not used in this check')
	elif check == 'equal':
			result = df.filter(df[column] == boundary)
			if boundary2 != None : v_print('Warning: Boundary2 is not used in this check')
	elif check == 'greater_equal':
			result = df.filter(df[column] >= boundary)
			if boundary2 != None : v_print('Warning: Boundary2 is not used in this check')
	elif check == 'less_equal':
			result = df.filter(df[column] <= boundary)
			if boundary2 != None : v_print('Warning: Boundary2 is not used in this check')
	elif check == 'not_equal':
			result = df.filter(df[column] != boundary)
			if boundary2 != None : v_print('Warning: Boundary2 is not used in this check')
	elif check == 'between':
			result = df.filter(df[column] >= boundary & df[column] <= boundary2)
	elif check == 'not_between':
			result = df.filter(df[column] < boundary & df[column] > boundary2)
	
	if result.count() > 0:
		text = 'checking ' + check + ' ' + column + ' ' + str(boundary) + ' ' + str(boundary2)
		v_print('Warning: %s values found in column %s' % (result.count(), column))
		exp = result.withColumn('check', F.lit(text))
		return result
	else:
		v_print('Info: Numeric evaluation finished, all values are correct')
		return None
		


def check_string(df, column:str , check:str, boundary = None, boundary2 = None, gravity = 'Warning'):
	checks = ['equal','not_equal','eq_len','not_eq_len','len_between','len_not_between','regex']
	if check not in checks:
		raise ValueError('Error: Invalid check type; expected one of: %s' % checks)
	# need to check if the column is string
	v_print('Warning: Verification of column type is not implemented yet')

	if check == 'equal':
		result = df.filter(df[column] == boundary)
		if boundary2 != None : v_print('Warning: Boundary2 is not used in this check')
	elif check == 'not_equal':
		result = df.filter(df[column] != boundary)
		if boundary2 != None : v_print('Warning: Boundary2 is not used in this check')
	elif check == 'eq_len':
		result = df.filter(F.length(df[column]) == boundary)
		if boundary2 != None : v_print('Warning: Boundary2 is not used in this check')
		# maybe the second boundary can be used as a second length option?
	elif check == 'not_eq_len':
		result = df.filter(F.length(df[column]) != boundary)
		if boundary2 != None : v_print('Warning: Boundary2 is not used in this check')
	elif check == 'len_between':
		result = df.filter(F.length(df[column]) >= boundary & F.length(df[column]) <= boundary2)
	elif check == 'len_not_between':
		result = df.filter(F.length(df[column]) < boundary & F.length(df[column]) > boundary2)
	elif check == 'regex':
		result = df.filter(df[column].rlike(boundary))
		if boundary2 != None : v_print('Warning: Boundary2 is not used in this check')
		# maybe the second boundary can be used as a second regex?
	
	if result.count() > 0:
		text = 'checking ' + check + ' ' + column + ' ' + str(boundary) + ' ' + str(boundary2) + ' ' + 'time: ' + str(int(datetime.datetime.now().timestamp()))
		v_print('Warning: %s values found in column %s' % (result.count(), column))
		exp = result.withColumn('check', F.lit(text))
		return result

	else:
		v_print('Info: String evaluation finished, all values are correct')
		return None


def eval_csv(df, path, sep = ',', header = 'true'):
	#expected schema: column_name, check_type, boundary, boundary2
	with open(path, 'r') as f:
		reader = csv.reader(f)
		for row in reader:
			if row[1] == 'numeric':
				result = check_numeric(df, row[0], row[2], row[3], row[4])
				if result != None:
					name = 'result_' + row[0] + '_' + row[1] + '_' + row[2] + '_' + row[3] + '_' + row[4] + '_' + str(int(datetime.datetime.now().timestamp())) + '.csv'
					result.toPandas().to_csv(name)
					v_print('Info: Evaluation finished, result saved to %s' % name)
				else:
					v_print('Info: Evaluation finished, all values are correct')
			elif row[1] == 'string':
				check_string(df, row[0], row[2], row[3], row[4])
				if result != None:
					name = 'result_' + row[0] + '_' + row[1] + '_' + row[2] + '_' + row[3] + '_' + row[4] + '_' + str(int(datetime.datetime.now().timestamp())) + '.csv'
					result.toPandas().to_csv(name)
					v_print('Info: Evaluation finished, result saved to %s' % name)
				else:
					v_print('Info: Evaluation finished, all values are correct')
			else:
				raise ValueError('Error: Invalid check type; expected one of: numeric, string')
	f.close()
	v_print('Info: Evaluation finished')