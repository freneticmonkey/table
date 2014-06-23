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
	
	def __init__(self, input_file=''):
		self.show_json = False
		self.data = []
		
		if not input_file is '':
			if input_file.endswith('json'):
				self.load_json(input_file)
			elif input_file.endswith('csv'):
				self.load_csv(input_file)
			else:
				print 'Unknown data format: ' + input_file

		self._reset()

	def load_json(self, file_name):
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
		self.data_format = {}
		self.data = new_data
		self._result = self.data

		if len(self.data) > 0:
			self.data_format = {key:type(value).__name__ for key, value in self.data[0].items() }

	# Decorator for pre-query setup
	def operation(f):
		def new_op(*args, **kwargs):
			self = args[0]
			if len(self.data) > 0:			
				if self.start_time is None:
					self._reset()
				if self._is_fluent:
					return f(*args, **kwargs)
				else:
					f(*args, **kwargs)
		return new_op

	def _reset(self):
		self.select_columns = []
		self.start_time = time()
		self._show_result = True
		self._is_fluent = True

		self._result = self.data
		self.rows_selected = 0
		self.el_time = 0

	@property
	def show(self):
	    return self._show_result
	@show.setter
	def show(self, value):
	    self._show_result = value

	@operation
	def export(self, file_name):
		if not os.path.isfile(file_name):
			with open(file_name,'w') as out:
				rows_exported = 0
				if self.start_time is not None:
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
		tbl = PrettyTable(['Key','Type'])
		tbl.align['Key'] = 'l'
		tbl.align['Type'] = 'l'
		for key, value in self.data_format.items():
			tbl.add_row([key,value])
		print tbl
		return self

	@operation
	def select(self, columns=[]):
		if type(columns) is list:
			self.select_columns = columns
		else:
			self.select_columns.append(columns)
		return self

	@operation
	def where(self, func):
		self._result = [item for item in self._result if func(item)]
		return self

	def eq(self, key, value):
		return self.where(lambda item: item[key] == value)
	
	def ne(self, key, value):
		return self.where(lambda item: item[key] != value)

	def gt(self, key, value):
		return self.where(lambda item: item[key] > value)

	def lt(self, key, value):
		return self.where(lambda item: item[key] < value)

	def isin(self, key, value):
		return self.where(lambda item: item[key] in value)

	def notin(self, key, value):
		return self.where(lambda item: item[key] not in value)

	def like(self, key, search_expr):
		exp = re.compile(search_expr)
		return self.where(lambda item: exp.search(str(item[key])) != None )

	def notlike(self, key, search_expr):
		exp = re.compile(search_expr)
		return self.where(lambda item: exp.search(str(item[key])) == None )

	@operation
	def orderby(self, key=True, reverse=False):
		self._result = sorted(self._result, key=itemgetter(key), reverse=reverse)
		return self

	@operation
	def groupby(self, key):
		res = defaultdict(list)
		for item in self._result: res[item[key]].append(item)
		self._result = [item for group in res.values() for item in group]
		return self

	@operation
	def limit(self, lim):
		self._result = self._result[:lim]
		return self

	@operation
	def count(self, col=None):
		if col is None:
			self._result = len(self._result)
		else:
			c = len([1 for item in self._result if hasattr(item,col)])
			self._result = c
		return self

	@operation
	def sum(self, prop):
		total = 0
		for item in self._result:
			total += item[prop]
		self._result = total
		return self

	@operation
	def distinct(self, prop):
		self._result = list({item[prop] : item for item in self._result}.values())
		return self

	@operation
	def join(self, other, column):
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
		self.show_json = True
		return self

	def _show(self):
		stats = ""
		if self.show_json:
			print json.dumps(item, sort_keys=True, indent=4, separators=(',',': '))
		else:
			if type(self._result) is list:
				if _prettytable:
					tbl = PrettyTable(self._result[0].keys())

					for item in self._result:
						tbl.add_row(item.values())

					if len(self.select_columns) > 0:
						print tbl.get_string(fields=self.select_columns)
					else:
						print tbl
				else:
					if len(self.select_columns) > 0:
						for col in self.select_columns:
							print col

						for item in self._result:
							for col in self.select_columns:
								print item[col] + '\t'
					else:
						for item in self._result:
							print item.values()

				stats = "Rows: %d" % len(self._result)
			else:
				stats = "Result: %s" % str(self._result)

		print "%s in (%3.3fs)" % (stats, self.el_time)

	@operation
	def result(self, show=True):
		if type(self._result) is list:
			# if columns have been selected and the result contains a dictionary
			if len(self.select_columns) > 0 and type(self._result[0]) is dict:
				# strip unnecessary columns
				self._result = [{i:item[i] for i in item if i in self.select_columns} for item in self._result]
			self.rows_selected = len(self._result)
		
		# Calculate processing time and reset
		self.el_time = time() - self.start_time
		self.start_time = None

		# Display the Result
		if show:
			self._show()

		# Return the result
		return self._result

	def __enter__(self):
		self._reset()
		# Disabling operation returns while in with block
		self._is_fluent = False

	def __exit__(self, type, value, traceback):
		# Enabling operation return for final result
		self._is_fluent = True
		return self.result(self._show_result)

	@operation
	def __call__(self, show=True):
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