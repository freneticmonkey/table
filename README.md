table
=====

Simple tabular data manipulation library which emulates basic relational database table operations.  This library assists with quickly filtering and sorting json data.

Examples
--------
Given simple source data in the format:
	
	students.json
	{"id":1,"name":"Steve","class_id":2,"paid":false}

#Loading data
	
	>>>from table import Table
	>>>tbl = Table('students.json')

or
	>>>tbl = Table()
	>>>tbl.load_json('students.json')
	Success: 10 items loaded in 0.001s

#Data Functions
Table has basic data functions, count, sum, min, max, avg.

To count the number of rows in a table

	>>>tbl.count()()
	Result: 10 in (0.000s)
	10

Counting a query result:

	>>>tbl.eq('class_id',2).count()()
	Result: 3 in (0.000s)
	3

Each query returns the results of the query.  In the cases above the number on the second line of the output is the integer value of count.

#Queries
The rows in the table can be queried in a similar manner to standard relational databases such as MySQL.  Available queries are: eq, ne, gt, lt, isin, notin, like, and notlike.

For example. To query the number of enrolled students in the classes with an id greater than 1:

	>>>tbl.eq('class_id',1).count()()
	Result: 8 in (0.000s)
	8

#Fluent Interface
Each of the operations return self so that a query can be built by chaining operations together.  This can be seen in the examples above where the eq() and gt() operations are chained to count().  An operation chain is terminated by calling the Table object.

#With Block
Table instances can also be used inside a with block.  For example:

	>>>with Table('students.json') as tbl:
	...    tbl.eq('class_id',2)
	...	   tbl.eq('paid',True)
	...
	Success: 10 items loaded in 0.000s
	+----------+----+------+----------+
	| class_id | id | paid |   name   |
	+----------+----+------+----------+
	|    2     | 2  | True | Caroline |
	|    2     | 5  | True |  Rupert  |
	+----------+----+------+----------+
	Rows: 2 in (0.000s)

#Sorting
Table has implementations of groupby and orderby.

	>>>tbl.orderby('class_id')
	+----------+----+-------+-----------+
	| class_id | id |  paid |    name   |
	+----------+----+-------+-----------+
	|    1     | 3  |  True |   Frank   |
	|    1     | 6  |  True |  Guiseppe |
	|    2     | 1  | False |   Steve   |
	|    2     | 2  |  True |  Caroline |
	|    2     | 5  |  True |   Rupert  |
	|    3     | 4  | False |  Jacinta  |
	|    3     | 7  | False |   Penny   |
	|    3     | 8  | False |   Maria   |
	|    3     | 9  | False |  Prudence |
	|    3     | 10 | False | Ferdinand |
	+----------+----+-------+-----------+
	Rows: 10 in (0.000s)

#Joining tables
Left and right joins can be used to pull the data from two tables together.

To query the names of classes for which students have paid:

	students = Table("students.json")
	classes = Table("classes.json")

	students.join(classes,'class_id','c').eq('paid',True).distinct('c.name')()

	+------------+----------+------+---------+----+----------+
	| c.class_id | class_id | paid |  c.name | id |   name   |
	+------------+----------+------+---------+----+----------+
	|     1      |    1     | True |  french | 6  | Guiseppe |
	|     2      |    2     | True | italian | 5  |  Rupert  |
	+------------+----------+------+---------+----+----------+
	Rows: 2 in (0.000s)

#Exporting Tables
The results of queries can also be written to files as JSON.

The following exports the results of the query above to a new file 'paid.json'.

	students.join(classes,'class_id','c').eq('paid',True).distinct('c.name').export('paid.json')(False)

Calling the Table object with a False parameter tells the Table object to not display the result which is helpful when analysing large datasets.