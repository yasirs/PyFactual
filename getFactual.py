#
# adapted from getFactual by Mike Borozdin
# @mikebz
#
# please keep the author's name in the code file if you are using
# this IP
# 
import json
from httplib import HTTPConnection
from pprint import pprint
import sys
import string
import urllib
from array import array
from django.utils.encoding import smart_str, smart_unicode

try:
	api_key = open('api.key').read().rstrip()
except:
	raise IOError, "need api.key file with factual.com api key"


def add_table_row(table, row_dict):

	conn = HTTPConnection("api.factual.com")
	query = "/v2/tables/" + table + "/input?"
	valstr = "{"
	for k,v in row_dict.iteritems():
		if k!= 'subject_key': # subject key should not be given
			valstr += "\""+str(k)+"\":\""+str(v)+"\","
	valstr = valstr[:-1] + "}"
	valstr=urllib.quote(valstr)
	query += "values="+valstr
	conn.request("GET", query + "&api_key=" + api_key)
	resp = conn.getresponse()
	status = resp.status
	if status == 200 or status == 304:
		return json.loads(resp.read())
	else:
		raise IOError, "Couldn't write to table %s, Error status code: %i" %(table,status)
		return json.loads(resp.read())
	
	

def get_table_data(table, search = ""):
	"""
	get_table_data(table, search = "") returns a table
	"""
	
	url = "http://api.factual.com"
	conn = HTTPConnection("api.factual.com")
	
	query = "/v2/tables/" + table + "/read?"
	if( search != "" ):
		query += "filters={\"$search\":\"" + search + "\"}&"		
	
	conn.request("GET", query + "api_key=" + api_key)
	
	resp = conn.getresponse()
	status = resp.status
	if status == 200 or status == 304:
		return json.loads(resp.read())
	else:
		raise IOError, "Couldn't read table %s, Error status code: %i" %(table,status)
		return json.loads(resp.read())
	
def get_table_schema(table):
	url = "http://api.factual.com"
	conn = HTTPConnection("api.factual.com")
	conn.request("GET", "/v2/tables/" + table + "/schema?api_key=" + api_key)
	resp = conn.getresponse()
	status = resp.status
	if status == 200 or status == 304:
		return json.loads(resp.read())
	else:
		raise IOError, "Couldn't read table %s schema, Error status code: %i" %(table,status)
		
	return {}

def print_table(data, col_ids = [] ):
	""" print all the data returned by get_table_data(table) or specific columns

	"""
	

	for row in data['response']['data']:
		
		for i in range( len(row) if col_ids==[] else min(len(row), len(col_ids))):
			sys.stdout.write('-------------------------------')
		sys.stdout.write("\n")
		
		for i in range(1, len(row) ): #don't print the subject key
			cell = row[i]
			
 			# handle the case when only certain col ids
 			# were selected
 			# NOTE: adjusting for subject id
 			if( len(col_ids) == 0 or (i-1) in col_ids):
	 			value = smart_str(cell)
 				if( len(value) > 30):
 					value = value[:26] + '...'
 				sys.stdout.write( "|{0:29}".format(value))
		sys.stdout.write("|\n")
 		
 		
'''
this function will take a table and fields and return the columns for those ids
'''
def table_row_lookup(table_id, fields):
	
	#DEBUG
	#print 'in table_row_lookup'
	#pprint(fields)
	
	columns = []; #create an empty collection to append
	result = get_table_schema(table_id) #this returns a schema part of which is fields
 	schema_data = json.loads(result)
 	
 	i = 0
 	#
 	# for every field see if it's also in the "fields" paramter
 	# if it is then append it's index, if not then just move on.
 	for field in schema_data['schema']['fields']:
 	
 		#DEBUG
		#print 'field:'
		#pprint(field)
	
 		if field['name'] in fields:
 			columns.append(i)
 		i += 1
 	
 	#DEBUG
 	#print 'return'
 	#pprint (columns)
 	
 	return columns
 		
