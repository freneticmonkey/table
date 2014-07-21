from table import Table

def test_load():
	assert Table("students.json").data != []

def test_count():
	classes = Table("classes.json")
	assert classes.count()(False) == 3

def test_distinct():
	students = Table("students.json")
	assert students.distinct("class_id").count()(False) == 3

def test_eq():
	students = Table("students.json")
	assert students.eq("class_id",2).count()(False) == 3

def test_ne():
	students = Table("students.json")
	assert students.ne("class_id",2).count()(False) == 7

def test_gt():
	students = Table("students.json")
	assert students.gt("class_id",2).count()(False) == 5

def test_lt():
	students = Table("students.json")
	assert students.lt("class_id",2).count()(False) == 2

def test_isin():
	students = Table("students.json")
	assert students.isin("class_id",[2,3]).count()(False) == 8
	
def test_notin():
	students = Table("students.json")
	assert students.notin("class_id",[1,2]).count()(False) == 5

def test_like():
	students = Table("students.json")
	assert students.like("name",".*n.*").count()(False) == 6

def test_notlike():
	students = Table("students.json")
	assert students.notlike("name","^G.*").count()(False) == 9

def test_orderby():
	result = [
		{u'class_id': 1, u'id': 3, 	u'paid': True, 	u'name': u'Frank'}, 
		{u'class_id': 1, u'id': 6, 	u'paid': True, 	u'name': u'Guiseppe'}, 
		{u'class_id': 2, u'id': 1, 	u'paid': False, u'name': u'Steve'}, 
		{u'class_id': 2, u'id': 2, 	u'paid': True, 	u'name': u'Caroline'}, 
		{u'class_id': 2, u'id': 5, 	u'paid': True, 	u'name': u'Rupert'}, 
		{u'class_id': 3, u'id': 4, 	u'paid': False, u'name': u'Jacinta'}, 
		{u'class_id': 3, u'id': 7, 	u'paid': False, u'name': u'Penny'}, 
		{u'class_id': 3, u'id': 8, 	u'paid': False, u'name': u'Maria'}, 
		{u'class_id': 3, u'id': 9, 	u'paid': False, u'name': u'Prudence'}, 
		{u'class_id': 3, u'id': 10, u'paid': False, u'name': u'Ferdinand'}
	]
	students = Table("students.json")
	assert students.orderby("class_id")(False) == result

def test_limit():
	students = Table("students.json")
	assert students.limit(2).count()(False) == 2

def test_sum():
	students = Table("students.json")
	assert students.sum("id")(False) == 55

def test_min():
	students = Table("students.json")
	assert students.min("id")(False) == 1

def test_max():
	students = Table("students.json")
	assert students.max("id")(False) == 10

def test_avg():
	students = Table("students.json")
	assert students.avg("id")(False) == 5

def test_join():
	students = Table("students.json")
	classes = Table("classes.json")

	result = [
		{u'c.class_id': 2, u'class_id': 2, u'paid': False, 	u'c.name': u'italian', 	u'id': 1, 	u'name': u'Steve'}, 
		{u'c.class_id': 2, u'class_id': 2, u'paid': True, 	u'c.name': u'italian', 	u'id': 2, 	u'name': u'Caroline'}, 
		{u'c.class_id': 1, u'class_id': 1, u'paid': True, 	u'c.name': u'french', 	u'id': 3, 	u'name': u'Frank'}, 
		{u'c.class_id': 3, u'class_id': 3, u'paid': False, 	u'c.name': u'spanish', 	u'id': 4, 	u'name': u'Jacinta'}, 
		{u'c.class_id': 2, u'class_id': 2, u'paid': True, 	u'c.name': u'italian', 	u'id': 5, 	u'name': u'Rupert'}, 
		{u'c.class_id': 1, u'class_id': 1, u'paid': True, 	u'c.name': u'french', 	u'id': 6, 	u'name': u'Guiseppe'}, 
		{u'c.class_id': 3, u'class_id': 3, u'paid': False, 	u'c.name': u'spanish', 	u'id': 7, 	u'name': u'Penny'}, 
		{u'c.class_id': 3, u'class_id': 3, u'paid': False, 	u'c.name': u'spanish', 	u'id': 8, 	u'name': u'Maria'}, 
		{u'c.class_id': 3, u'class_id': 3, u'paid': False, 	u'c.name': u'spanish', 	u'id': 9, 	u'name': u'Prudence'}, 
		{u'c.class_id': 3, u'class_id': 3, u'paid': False, 	u'c.name': u'spanish', 	u'id': 10, 	u'name': u'Ferdinand'}
	]
	assert students.join(classes, "class_id", "c")(False) == result

def test_rjoin():
	students = Table("students.json")
	classes = Table("classes.json")

	result = [
		{u's.name': u'Frank', 		u'name': u'french', u'class_id': 1, u's.class_id': 1, u's.id': 3, u's.paid': True}, 
		{u's.name': u'Guiseppe', 	u'name': u'french', u'class_id': 1, u's.class_id': 1, u's.id': 6, u's.paid': True}, 
		{u's.name': u'Steve', 		u'name': u'italian', u'class_id': 2, u's.class_id': 2, u's.id': 1, u's.paid': False}, 
		{u's.name': u'Caroline', 	u'name': u'italian', u'class_id': 2, u's.class_id': 2, u's.id': 2, u's.paid': True}, 
		{u's.name': u'Rupert', 		u'name': u'italian', u'class_id': 2, u's.class_id': 2, u's.id': 5, u's.paid': True}, 
		{u's.name': u'Jacinta', 	u'name': u'spanish', u'class_id': 3, u's.class_id': 3, u's.id': 4, u's.paid': False}, 
		{u's.name': u'Penny', 		u'name': u'spanish', u'class_id': 3, u's.class_id': 3, u's.id': 7, u's.paid': False}, 
		{u's.name': u'Maria', 		u'name': u'spanish', u'class_id': 3, u's.class_id': 3, u's.id': 8, u's.paid': False}, 
		{u's.name': u'Prudence', 	u'name': u'spanish', u'class_id': 3, u's.class_id': 3, u's.id': 9, u's.paid': False}, 
		{u's.name': u'Ferdinand', 	u'name': u'spanish', u'class_id': 3, u's.class_id': 3, u's.id': 10, u's.paid': False}
	]

	assert students.rjoin(classes, "class_id", "s")(False) == result

def test_compound():
	students = Table("students.json")
	classes = Table("classes.json")

	assert students.join(classes,'class_id','c').eq('paid',True).distinct('c.name').count()(False) == 2