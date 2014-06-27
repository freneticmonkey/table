"""
The MIT License (MIT)

Copyright (c) 2014 Scott Porter

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

# Python Includes
import json, re, calendar, os, copy, csv
from time import time
from datetime import datetime
from operator import itemgetter
from collections import defaultdict

# Package includes
_prettytable = False
try:
	from prettytable import PrettyTable
	_prettytable = True
except Exception, e:
	print "Unable to import prettytable. install using 'easy_install prettytable'"
	print e.message

class Table(object):
	"""A very basic tabular data manipulation class
	
	If the class has public attributes, they should be documented here
	in an ``Attributes`` section and follow the same formatting as a
	function's ``Args`` section.

	Attributes:
		data (list): Contains a 'list' of dictionaries each of which represents a 'row'
		_data_format (dict): a dictionary containing the name and format of each field in a 'row' dictionary
		_result (list): Contains the rsulting list of dictionaries after querying ``data``
		_select_columns 
		_start_time (time): Operation start time
		_show_result (time): Show the value of _result at the end of querying
		_is_fluent (bool): Used to determine if the object is being used inside a with block
		_rows_selected (int): The final number of 'rows' after querying
		_el_time (time): Operation elapsed time
	
	"""
	def __init__(self, input_file=None):
		"""Constructor. Will load file specified by input_parameter if supplied

		input_file (str): file containing tabular data (json or csv) (default: None)
		"""
		self.show_json = False
		self.data = []
		
		if input_file is not None:
			if input_file.endswith('json'):
				self.load_json(input_file)
			elif input_file.endswith('csv'):
				self.load_csv(input_file)
			else:
				print 'Unknown data format: ' + input_file

		self._reset()
	
	def _reset(self):
		"""resets the query state variables"""
		self._select_columns = []
		self._start_time = time()
		self._show_result = True
		self._is_fluent = True

		self._result = self.data
		self._rows_selected = 0
		self._el_time = 0

	def load_json(self, file_name):
		"""loads JSON formatted data into the table

		Keyword arguments:
		file_name (str): a filename for a JSON file
		"""
		if os.path.isfile(file_name):
			start = time()
			data = []
			with open(file_name, 'r') as f:
				for line in f.readlines():
					try:
						data.append(json.loads(line))
					except Exception, e:
						print "Load failed: " + e.message
						print "Data: " + line
						return
			self._setdata(data)
			print "Success: %d items loaded in %4.3fs" % (len(self.data), time() - start)
		else:
			print "Load Error.  File doesn't exist."

	def load_csv(self, file_name):
		"""loads CSV formatted data into the table

		Keyword arguments:
		file_name (str): a filename for a CSV file
		"""
		if os.path.isfile(file_name):
			start = time()
			data = []
			with csv.DictReader(open(file_name, 'r')) as d:
				for row in f.readlines():
					try:
						data.append(row)
					except Exception, e:
						print "Load failed: " + e.message
						print "Data: " + line
						return
			self._setdata(data)
			print "Success: %d items loaded in %4.3fs" % (len(self.data), time() - start)
		else:
			print "Load Error.  File doesn't exist."

	def _setdata(self, new_data):
		"""replaces the contents of the table with new_data

		Keyword arguments:
		new_data (list): the data which will replace the current data member
		"""
		self.data_format = {}
		self.data = new_data
		self._result = self.data

		if len(self.data) > 0:
			self.data_format = {key:type(value).__name__ for key, value in self.data[0].items() }

	# Decorator for pre-query setup
	def operation(f):
		"""decorator for managing query operations

		Keyword arguments:
		f (function): a query operatiom function
		"""
		def new_op(*args, **kwargs):
			"""determines if a query is already running and resets the query state members if it isn't

			Keyword arguments:
			args (list): 
			kwargs (list):
			
			Returns:
			None or the function return
			"""
			self = args[0]
			if len(self.data) > 0:			
				if self._start_time is None:
					self._reset()
				if self._is_fluent:
					return f(*args, **kwargs)
				else:
					f(*args, **kwargs)
		return new_op

	@property
	def show(self):
		"""get whether the query result will be displayed on completion
		
		Returns:
		bool: current state of _show_result
		"""
		return self._show_result
	@show.setter
	def show(self, value):
		"""set whether the query result will be displayed on completion"""
		self._show_result = value

	@operation
	def export(self, file_name):
		"""write the table or query result in JSON format, to the file specified by the parameter
		
		Keyword arguments:
		file_name (str): target filename for exporting the table
		
		Returns:
		object: self for fluent interface
		"""
		if not os.path.isfile(file_name):
			with open(file_name,'w') as out:
				rows_exported = 0
				if self._start_time is not None:
					for item in self._result:
						out.write('%s\n' % json.dumps(item))
				else:
					for item in self.data:
						out.write('%s\n' % json.dumps(item))
				print "Exported %d rows to: %s." % ( rows_exported, file_name)
		else:
			print "Export error: File already exists."

	@operation
	def desc(self):
		"""print the names and datatypes of the columns in the table
		
		Returns:
		object: self for fluent interface
		"""
		tbl = PrettyTable(['Key','Type'])
		tbl.align['Key'] = 'l'
		tbl.align['Type'] = 'l'
		for key, value in self.data_format.items():
			tbl.add_row([key,value])
		print tbl
		return self

	@operation
	def select(self, columns=[]):
		"""select the columns to return
		
		Keyword arguments:
		columns (list): list of names of the columns in the table
		
		Returns:
		object: self for fluent interface
		"""
		if type(columns) is list:
			self._select_columns = columns
		else:
			self._select_columns.append(columns)
		return self

	@operation
	def where(self, func):
		"""filter the _result list based on the return on the lambad function as defined by func
		
		Keyword arguments:
		func (lambda): a function which returns a boolean and accepts a dict param
		
		Returns:
		object: self for fluent interface
		"""
		self._result = [item for item in self._result if func(item)]
		return self

	def eq(self, key, value):
		"""compare the equality of the value of the column with the name ``key`` to the parameter ``value`` for each row in the table
		
		Keyword arguments:
		key (str): name of the column on which to execute the comparison
		value (object): comparative value
		
		Returns:
		object: self for fluent interface
		"""
		return self.where(lambda item: item[key] == value)
	
	def ne(self, key, value):
		"""compare the inequality of the value of the column with the name ``key`` to the parameter ``value`` for each row in the table
		
		Keyword arguments:
		key (str): name of the column on which to execute the comparison
		value (object): comparative value column
		
		Returns:
		object: self for fluent interface
		"""
		return self.where(lambda item: item[key] != value)

	def gt(self, key, value):
		"""compare where the value of column ``key`` is greater than parameter ``value`` for each row in the table
		
		Keyword arguments:
		key (str): name of the column on which to execute the comparison
		value (object): comparative value
		
		Returns:
		object: self for fluent interface
		"""
		return self.where(lambda item: item[key] > value)

	def lt(self, key, value):
		"""compare where the value of column ``key`` is less than parameter ``value`` for each row in the table
		
		Keyword arguments:
		key (str): name of the column on which to execute the comparison
		value (object): comparative value
		
		Returns:
		object: self for fluent interface
		"""
		return self.where(lambda item: item[key] < value)

	def isin(self, key, lst):
		"""compare where the value of column ``key`` is found in parameter list ``value`` for each row in the table
		
		Keyword arguments:
		key (str): name of the column on which to execute the comparison
		lst (list): a list containing values matching type column ``key``
		
		Returns:
		object: self for fluent interface
		"""
		return self.where(lambda item: item[key] in lst)

	def notin(self, key, lst):
		"""compare where the value of column ``key`` is not found in parameter list ``value`` for each row in the table
		
		Keyword arguments:
		key (str): name of the column on which to execute the comparison
		lst (list): a list containing values matching type column ``key``
		
		Returns:
		object: self for fluent interface
		"""
		return self.where(lambda item: item[key] not in lst)

	def like(self, key, search_expr):
		"""compare where the value of column ``key`` matches the regular expression ``search_expr``
		
		Keyword arguments:
		key (str): name of the column on which to execute the regular expression
		search_expr (string): a string containing a regular expression
		
		Returns:
		object: self for fluent interface
		"""
		exp = re.compile(search_expr)
		return self.where(lambda item: exp.search(str(item[key])) != None )

	def notlike(self, key, search_expr):
		"""compare where the value of column ``key`` does not match the regular expression ``search_expr``
		
		Keyword arguments:
		key (str): name of the column on which to execute the regular expression
		search_expr (string): a string containing a regular expression
		
		Returns:
		object: self for fluent interface
		"""
		exp = re.compile(search_expr)
		return self.where(lambda item: exp.search(str(item[key])) == None )

	@operation
	def orderby(self, key=True, reverse=False):
		"""order the rows in the table by the value of column ``key``
		
		Keyword arguments:
		key (str): name of the column on which to execute the order 
		reverse (bool): reverse order (default: False)
		
		Returns:
		object: self for fluent interface
		"""
		self._result = sorted(self._result, key=itemgetter(key), reverse=reverse)
		return self

	@operation
	def groupby(self, key):
		"""group the rows in table by column ``key``
		
		Keyword arguments:
		key (str): name of the column on which to execute the group by
		
		Returns:
		object: self for fluent interface
		"""
		res = defaultdict(list)
		for item in self._result: res[item[key]].append(item)
		self._result = [item for group in res.values() for item in group]
		return self

	@operation
	def limit(self, lim):
		"""limit the rows in the result set to ``lim`` rows
		
		Keyword arguments:
		lim (int): number of rows to restrict the result set to
		
		Returns:
		object: self for fluent interface
		"""
		self._result = self._result[:lim]
		return self

	@operation
	def count(self, col=None):
		"""count the number of rows in the result. if ``col`` parameter exists count col ``col``
		
		A column parameter is provided here to provide the abilty to inspect 
		if the internal representation of rows is correct. Because the table 
		structure is not enforced it is possible to have two rows with different columns.
		
		Keyword arguments:
		col (str): name of the column on which to execute the count
		
		Returns:
		object: self for fluent interface
		"""
		if col is None:
			self._result = len(self._result)
		else:
			c = len([1 for item in self._result if hasattr(item,col)])
			self._result = c
		return self

	@operation
	def sum(self, col):
		"""sum the values in the column ``col``
		
		Keyword arguments:
		col (str): name of the column on which to execute the count
		
		Returns:
		object: self for fluent interface
		"""
		total = 0
		for item in self._result:
			total += item[col]
		self._result = total
		return self

	@operation
	def distinct(self, col):
		"""limit the rows in the result set to rows with distinct values in the ``col`` column
		
		Keyword arguments:
		col (str): the name of the column on which to select distinct rows
		
		Returns:
		object: self for fluent interface
		"""
		self._result = list({item[col] : item for item in self._result}.values())
		return self

	@operation
	def join(self, other, column):
		"""join another table instance where the value of the ``column`` column is eqivalent
		
		Keyword arguments:
		other (Table): the Table on which to join
		column (str): the name of the column on which to join the two tables
		
		Returns:
		Table: a table instance containing the join result
		"""
		tmp = Table()
		tmp.data = []

		for item in self._result:
			for oitem in other._result:
				if item[column] == oitem[column]:
					n_item = copy.copy(item)
					for key, value in oitem.items():
						n_item[key+'_'] = value
					tmp.data.append(n_item)
		tmp._setdata(tmp.data)
		return tmp

	@operation
	def rjoin(self, other, column):
		"""join this table to another Table where the value of the ``column`` column is eqivalent
		
		Keyword arguments:
		other (Table): the Table on which to join this table
		column (str): the name of the column on which to join the two tables
		
		Returns:
		Table: a table instance containing the join result
		"""
		tmp = Table()
		tmp.data = []

		for oitem in other._result:
			for item in self._result:
				if item[column] == oitem[column]:
					n_item = copy.copy(oitem)
					for key, value in item.items():
						n_item[key+'_'] = value
					tmp.data.append(n_item)
		tmp._setdata(tmp.data)
		return tmp

	def format_json(self):
		"""set whether the result shown at the end of the query is in JSON format
		
		Returns:
		object: self for fluent interface
		"""
		self.show_json = True
		return self

	def _show(self):
		"""display the contents of the result list on screen.
		
		If PrettyTable is installed is will be used, if not the dictionaries in the list will just have their values printed
		"""
		stats = ""
		if self.show_json:
			print json.dumps(item, sort_keys=True, indent=4, separators=(',',': '))
		else:
			if type(self._result) is list:
				if _prettytable:
					tbl = PrettyTable(self._result[0].keys())

					for item in self._result:
						tbl.add_row(item.values())

					if len(self._select_columns) > 0:
						print tbl.get_string(fields=self._select_columns)
					else:
						print tbl
				else:
					if len(self._select_columns) > 0:
						for col in self._select_columns:
							print col

						for item in self._result:
							for col in self._select_columns:
								print item[col] + '\t'
					else:
						for item in self._result:
							print item.values()

				stats = "Rows: %d" % len(self._result)
			else:
				stats = "Result: %s" % str(self._result)

		print "%s in (%3.3fs)" % (stats, self._el_time)

	@operation
	def result(self, show=True):
		"""utility function to filter the columns in the result if required and to calculate the number of rows and elapsed time of the query
		
		Keyword arguments:
			show (bool): if true the results of the query will be displayed
			
		Returns:
		list: the result of the query
		"""
		if type(self._result) is list:
			# if columns have been selected and the result contains a dictionary
			if len(self._select_columns) > 0 and type(self._result[0]) is dict:
				# strip unnecessary columns
				self._result = [{i:item[i] for i in item if i in self._select_columns} for item in self._result]
			self._rows_selected = len(self._result)
		
		# Calculate processing time and reset
		self._el_time = time() - self._start_time
		self._start_time = None

		# Display the Result
		if show:
			self._show()

		# Return the result
		return self._result

	def __enter__(self):
		"""handle the beginning of a with block. 
		
		Starts a query and disables the fluent interface
		so that using in a console window won't result in 
		the table object being printed 
		"""
		self._reset()
		# Disabling operation returns while in with block
		self._is_fluent = False

	def __exit__(self, type, value, traceback):
		"""handle the end of a with block. 
		
		Finishes the query and enables the fluent interface 
		
		Returns:
		list: the result of the query
		"""
		# Enabling operation return for final result
		self._is_fluent = True
		return self.result(self._show_result)

	@operation
	def __call__(self, show=True):
		"""Finish a query when using the fluent interface
		
		Returns:
		list: the result of the query
		"""
		# Process the final operations
		return self.result(show)

if __name__== "__main__":
	
	students = Table()
	students.load_json("students.json")

	classes = Table("classes.json")

	# Verbose operations
	# students.where(lambda i: i['class_id'] > 1).count()()
	# students.where(lambda i: i['paid'] == True).count()()

	# students.gt('class_id',1).count()()
	# students.eq('paid',True).count()()
	# students.isin('class_id',[1,2]).count()()
	# students.like('name','.*n.*').count()()

	# students.gt('class_id',1).limit(2).select(['name','paid'])()

	students.gt('class_id',1).limit(5).groupby('class_id').distinct('class_id')()

	# classes which have students who have paid
	students.join(classes,'class_id').eq('paid',True).distinct('name_')()