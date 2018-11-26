from __future__ import print_function

import mysql.connector
from mysql.connector import errorcode
from emission.core.sql_table_fields import *
from emission.core.sql_utilities import *

ER_TABLE_EXISTS_ERROR = 1105
ROLLBACK_ERROR = 1213

class SQLManager:
	def __init__(self):
		self.connection = mysql.connector.connect(
			host="cryptdb",
			user="root",
			passwd="letmein",
			port="3307",
			autocommit=True
		)
		self.cursor = self.connection.cursor(dictionary=True)
		self.use_database()
		self.create_tables()
		self.table = None
		print('after init sql')

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.commit()
		self.cursor.close()
		self.connection.close()

	def commit(self):
		return
		# self.connection.commit()
		# self.cursor.close()
		# self.cursor = self.connection.cursor(dictionary=True)

	def set_table(self, table):
		if table != self.table:
			self.table = table

	# select fields from table where param = something;
	#fields = [field]
	#params = {param1:val, param2:val}
	def query(self, fields, paramsDic):
		if self.table is None:
			return None
		elif type(fields) == list and len(fields) > 1:
			fields = ", ".join(fields)
		sql = "select " + fields + " from " + self.table
		if paramsDic is not None:
			sql += " where "

			if "direct_query" in paramsDic:
				direct_query = paramsDic["direct_query"]
				del paramsDic["direct_query"]
			else:
				direct_query = None
			parameters = [key +" = \'"+ str(paramsDic[key]) + "\'" for key in paramsDic.keys()]
			sql += " and ".join(parameters)
			if direct_query is not None:
				sql += " and " + direct_query
		#print('querying: ', sql)
		print('querying')
		try:
			self.cursor.execute(sql)
			return True
		except mysql.connector.Error as err:
			print(err.msg, err.errno) #rollback error bc no results
			return False
		# if self.cursor.rowcount <= 0:
		# 	return None
		
	def find(self, paramsDic, fields="*"):
		success = self.query(fields, paramsDic)
		if not success:
			return []
		return self.fetchall()

	def find_one(self, paramsDic, fields="*"):
		success = self.query(fields, paramsDic)
		if not success:
			return None
		result = self.fetchone()
		return result

	def find_distinct(self, paramsDic, fields):
		if fields is None:
			return None
		if type(fields) == list and len(fields) > 1:
			fields = ", ".join(fields)
		# success = self.query("distinct " +fields, paramsDic)
		# if not success:
		# 	return None
		# return self.fetchall()
		if self.table is None:
			return None
		elif type(fields) == list and len(fields) > 1:
			fields = ", ".join(fields)
		sql = "select distinct " + fields + " from " + self.table
		if paramsDic is not None:
			sql += " where "
			parameters = [key +" = \'"+ str(paramsDic[key]) + "\'" for key in paramsDic.keys()]
			sql += " and ".join(parameters)
		sql += " group by " + fields
		#print('querying distinct: ', sql)
		print('querying distinct')
		try:
			self.cursor.execute(sql)
			return self.fetchall()
		except mysql.connector.Error as err:
			print(err.msg, err.errno) #rollback error bc no results
			return None


	# update table set field = val where param = something;
	#fields to be updated = {field1:val, field2:val}
	#params to select which rows to update = {param1:val, param2:val}

	#update shouldn't create new rows
	def update(self, paramsDic, fieldsDic):
		if self.table is None or fieldsDic is None:
			return None
		fields = [key+" = '"+str(fieldsDic[key]) + "'" for key in fieldsDic.keys()]
		sql = "update " + self.table + " set " + ", ".join(fields)
		if paramsDic is not None:

			sql += " where "
			parameters = [key +" = '"+str(paramsDic[key]) + "'" for key in paramsDic.keys()]
			sql += " and ".join(parameters)
		#print('update: ', sql)
		print('update')
		self.cursor.execute(sql)
		self.commit()

	# insert into table (columns, columns) values (vals, vals) on duplicate key update field=val, field=val

	# try insert params as row, if duplicate then update fields = {field1:val, field2:val}
	def insert_or_update(self, paramsDic, fieldsDic):
		if self.table is None or fieldsDic is None:
			return None
		sql = "insert into " + self.table + " ("

		keys = list(paramsDic.keys())
		sql += ", ".join(keys) + ") values ("
		
		strings = table_string[self.table]

		vals = []
		for i in range(len(keys)):
			key = keys[i]
			if key in strings or key in metadata_string:
				vals.append("'" + str(paramsDic[key]) + "'")
			else:
				vals.append(str(paramsDic[key]))
		vals = ", ".join(vals)
		
		sql += vals + ") on duplicate key update "

		fields, fieldsKeys = [], list(fieldsDic.keys())
		for i in range(len(fieldsKeys)):
			key = fieldsKeys[i]
			if key in strings or key in metadata_string:
				fields.append(key+" = '" + str(fieldsDic[key]) + "'")
			else:
				fields.append(key+" = " + str(fieldsDic[key]))

		sql += ", ".join(fields)
		sql = sql.replace("'None'", "NULL")
		sql = sql.replace("None", "NULL")

		try:
			#print('insert_or_update: ', sql)
			print('insert_or_update')
			self.cursor.execute(sql)
			self.commit()
		except mysql.connector.Error as err:
			print(err.msg)

	def insertToTable(self, table, dictionary):
		sql = 'INSERT INTO ' + table + ' ('
		keys = list(dictionary.keys())

		strings = table_string[table]

		vals = []
		for i in range(len(keys)):
			key = keys[i]
			if key in strings or key in metadata_string:
				vals.append("'" + str(dictionary[key]) + "'")
			else:
				vals.append(str(dictionary[key]))

		vals = ", ".join(vals)
		vals = vals.replace("'None'", "NULL")
		vals = vals.replace("None", "NULL")
		
		keys = ", ".join(keys)
		sql += keys + ") values ("
		sql += vals + ")"
		try:
			#print("insert: ", sql)
			print("insert")
			self.cursor.execute(sql)
			self.commit()
		except mysql.connector.Error as err:
			print(err.msg)

	def insert_one(self, entry):
		dictionary = self.flatten_json(entry)
		if dictionary['metadata_key'] in metadata_key_to_table:
			self.insertToTable(metadata_key_to_table[dictionary['metadata_key']], dictionary)
		return dictionary

	# return database rows in form of dictionary
	def fetchall(self):
		# if self.cursor.rowcount <= 0:
		# 	return None
		if self.cursor._have_unread_result():
			results = self.cursor.fetchall()
		else:
			results = None
		if results is None or len(results) == 0:
			return None
		keys = list(results[0].keys())
		for result in results:
			for key in keys:
				if result[key] == "NULL":
					result[key] = None
		return results
		#return [dict(zip([col[0] for col in self.cursor.description], row)) for row in result_rows]

	def fetchone(self):
		# if self.cursor.rowcount <= 0:
		# 	return None
		result = self.cursor.fetchone()
		if result is None:
			return None
		keys = list(result.keys())
		for key in keys:
			if result[key] == "NULL":
				result[key] = None
		return result

	def create_database(self):
		try:
			self.cursor.execute(
				"CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
		except mysql.connector.Error as err:
			print("Failed creating database: {}".format(err))
			exit(1)

	# try to use database; or create new database named DB_NAME
	def use_database(self):
		try:
			self.cursor.execute("USE {}".format(DB_NAME))
			self.commit()
		except mysql.connector.Error as err:
			print("Database {} does not exists.".format(DB_NAME))
			self.create_database()
			print("Database {} created successfully.".format(DB_NAME))
			self.connection.database = DB_NAME

	# try to create table, except if it already exists
	def create_tables(self):
		for table_name in TABLES:
			table_description = TABLES[table_name]
			try:
				print("Creating table {} \n".format(table_name), end='')
				self.cursor.execute(table_description)
				self.commit()
			except mysql.connector.Error as err:
				return
				# if err.errno == ER_TABLE_EXISTS_ERROR:
				# 	print("already exists.")
				# else:
				#print(err.msg)

	#flatten json
	def flatten_json(self, y):
		out = {}
		def flatten(x, name=''):
			if type(x) is dict:
				for a in x:
					flatten(x[a], name + a + '_')
			# elif type(x) is list:
			# 	i = 0
			# 	for a in x:
			# 		flatten(a, name + str(i) + '_')
			# 		i += 1
			else:
				out[name[:-1]] = x
		flatten(y)
		return out

# sql = SQLManager()