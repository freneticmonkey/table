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
import json, re, calendar
from time import time
from datetime import datetime
from operator import itemgetter
from collections import defaultdict

# Package includes
import sys
sys.path.append('c:/python27/lib/site-packages')
from prettytable import PrettyTable

class Table(object):
	
	def __init__(self, input_file=''):
		self.show_json = False
		self.data = []
		
		if not input_file is '':
			self.load(input_file)

		self._reset()

	def load(self, input_file):
		self.data_format = {}
		self.data = []
		self.have_format = False
		start = time()
		with open(input_file, 'r') as f:
			for line in f.readlines():
				try:
					row = json.loads(line)
					self.data.append(row)
					if not self.have_format:
						for key, value in row.items():
							self.data_format[key] = type(value).__name__

				except Exception, e:
					print "Load failed: " + e.message
					print "Data: " + line
					self.data = []
					return

		print "Success: %d items loaded in %4.3fs" % (len(self.data), time() - start)
		self._result = self.data

	# Decorator for pre-query setup
	def operation(f):
		def new_op(*args, **kwargs):
			self = args[0]
			if len(self.data) > 0:			
				if self.start_time is None:
					self._reset()
				return f(*args, **kwargs)
		return new_op

	def _reset(self):
		self.select_columns = []
		self.start_time = time()

		self._result = self.data
		self.rows_selected = 0
		self.el_time = 0

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
	def count(self, enabled=True):
		self._result = len(self._result)
		return self

	@operation
	def sum(self, prop):
		total = 0
		for item in self._result:
			total += item[prop]
		self._result = total
		return self

	@operation
	def dist(self, prop):
		self._result = list({item[prop] : item for item in self.data}.values())
		return self		

	def format_json(self):
		self.show_json = True
		return self

	def _show(self):
		stats = ""
		if self.show_json:
			print json.dumps(item, sort_keys=True, indent=4, separators=(',',': '))
		else:
			if type(self._result) is list:
				tbl = PrettyTable(self._result[0].keys())

				for item in self._result:
					tbl.add_row(item.values())

				if len(self.select_columns):
					print tbl.get_string(fields=self.select_columns)
				else:
					print tbl

				stats = "Rows: %d" % len(self._result)
			else:
				stats = "Result: %s" % str(self._result)

		print "%s in (%3.3fs)" % (stats, self.el_time)

	@operation
	def result(self):
		if type(self._result) is list:
			# if columns have been selected and the result contains a dictionary
			if len(self.select_columns) > 0 and type(self._result[0]) is dict:
				# strip unnecessary columns
				self._result = [{i:item[i] for i in item if i in self.select_columns} for item in self._result]
			self.rows_selected = len(self._result)
		return self._result

	@operation
	def __call__(self):
		# Process the final operations
		self.result()
		
		# Calculate processing time and reset
		self.el_time = time() - self.start_time
		self.start_time = None

		# Display the Result
		self._show()

		# Return the result
		return self._result

if __name__== "__main__":
	
	tbl = Table()
	tbl.load("students.json")

	# Verbose operations
	# tbl.where(lambda i: i['class_id'] > 1).count()()
	# tbl.where(lambda i: i['paid'] == True).count()()

	# tbl.gt('class_id',1).count()()
	# tbl.eq('paid',True).count()()
	# tbl.isin('class_id',[1,2]).count()()
	# tbl.like('name','.*n.*').count()()

	# tbl.gt('class_id',1).limit(2).select(['name','paid'])()

	tbl.gt('class_id',1).limit(5).groupby('class_id')()