from py2neo import neo4j
import time

start_time=time.time()
uri = "http://localhost:8001/db/data/"
graph_db = neo4j.GraphDatabaseService(uri)

# node = graph_db.node(12)
# print type(node)
# index = graph_db.get_or_create_index(neo4j.Node,'userIdIndex')
# print index

# indexes = graph_db.get_indexes(neo4j.Node)
# print indexes
#a = graph_db.get_indexed_node("userIdIndex","id","6170731")
# a = graph_db.get_indexed_node("userIdIndex","id","6170731")
# a = graph_db.get_indexed_node("workIndex","id","1")
# print a

# b = graph_db.get_indexed_node("employerIdIndex","id","105454359493345")
# print b
# print graph_db.neo4j_version
# a = graph_db.node(7)
# print a.get_properties()
# print a.get_properties()['nodeType'] == 1
# c = graph_db.get_or_create_index(neo4j.Node, "employerIdIndex")
# print c
# tmp = graph_db.node(7)
# print tmp.get_properties()['nodeType']




#from first 10000 nodes
#{0: 3503, 1: 3760, 2: 2326, 3: 1, 4: 1, 5: 1, 6: 0, 7: 0}
# store = {0:0,1:0,2:0,3:0,4:0,5:0,6:0,7:0}
# for i in range(1,10000): #node 0 does not have nodeType
# 	# print i
# 	try:
# 		tmp = graph_db.node(i).get_properties()	
# 	except:
# 		print "No data"
# 		continue

# 	store[tmp["nodeType"]] += 1
# print store
#from first 1000 who are co-workers, list of companies
# companies = []
# for i in range(1,1000):
# 	try:
# 		tmp = graph_db.node(i).get_properties()	
# 	except:
# 		print "No data"
# 		continue

# 	if tmp["nodeType"] == 1:
# 		companies.append(tmp["name"])
# print set(companies)
##############################################
typechart = {"Type1":0,"Type2":0,"Type3":0,"Type4":0,"Type5":0,"Type6":0,"Type7":0,"Type8":0}
# human = []
# for i in Xrange(1,100):
# 	try:
# 		tmp = graph_db.node(i).get_properties()	
# 	except:
# 		print "No data"
# 		continue
# 	if tmp["nodeType"] == 0:
# 		human.append(i)
# print human

def typeCount(typechart,node):
	# node = graph_db.node(nodeID)
	school = {"College":[],"High School":[],"Graduate School":[]}
	schools = list(node.match_outgoing(rel_type = "STUDIED", end_node = None))
	for i in xrange(0,len(schools)):
		rel = graph_db.relationship(schools[i]._id)
		school[rel.get_properties()["educationType"]].append(schools[i].end_node.get_properties()["name"])
	print school
	company = []
	#WORKED_AT
	companies = list(node.match_outgoing(rel_type = "WORKED_AT", end_node = None))
	for i in xrange(0,len(companies)):
		company.append(companies[i].end_node.get_properties()["name"])

	#WORKS_AT
	companies = list(node.match_outgoing(rel_type = "WORKS_AT", end_node = None))
	for i in xrange(0,len(companies)):
		company.append(companies[i].end_node.get_properties()["name"])
	print company

	#CONNECTED NODES
	connections = list(node.match_outgoing(rel_type = "CONNECTED", end_node = None)) + \
	list(node.match_incoming(rel_type = "CONNECTED"))
	connectionschool = {"College":[],"High School":[],"Graduate School":[]}
	connectioncompany = []
	print "This is connections size:"
	print len(connections)
	#if end node is itself, use start node!!!!
	for i in xrange(0,len(connections)):
		if connections[i].end_node == node:
			print "node and end node are the same!"
			connectioncompanies = list(connections[i].start_node.match_outgoing(rel_type = "WORKS_AT", end_node = None))
		else:
			connectioncompanies = list(connections[i].end_node.match_outgoing(rel_type = "WORKS_AT", end_node = None))
		print "This is connection companies"
		print len(connectioncompanies)

		#works_at
		for j in xrange(0,len(connectioncompanies)):
			connectioncompany.append(connectioncompanies[j].end_node.get_properties()["name"])
		#worked_at
		if connections[i].end_node == node:
			print "node and end node are the same!"
			connectioncompanies = list(connections[i].start_node.match_outgoing(rel_type = "WORKED_AT", end_node = None))
		else:
			connectioncompanies = list(connections[i].end_node.match_outgoing(rel_type = "WORKED_AT", end_node = None))
		for k in xrange(0,len(connectioncompanies)):
			connectioncompany.append(connectioncompanies[k].end_node.get_properties()["name"])

		if connections[i].end_node == node:
			print "node and end node are the same!"
			connectionschools = list(connections[i].start_node.match_outgoing(rel_type = "STUDIED", end_node = None))
		else:
			connectionschools = list(connections[i].end_node.match_outgoing(rel_type = "STUDIED", end_node = None))

		for l in xrange(0,len(connectionschools)):
			rel = graph_db.relationship(connectionschools[l]._id)
			try:
				connectionschool[rel.get_properties()["educationType"]].append(connectionschools[l].end_node.get_properties()["name"])
			except:
				continue
		# print "connection high school"
		# print set(connectionschool["High School"])
	# print connectionschool
	# print connectioncompany
		highschool = list(set(school["High School"]) & set(connectionschool["High School"]))
		college = list(set(school["College"]) & set(connectionschool["College"]) & set(connectionschool["Graduate School"]))
		company_intercept = list(set(company) & set(connectioncompany))
		print "Here are the interceptions:"
		print highschool
		print college
		print company_intercept
		if len(highschool)!=0:
			typechart["Type2"]+=1
			print "same highschool"
		elif len(college)!=0:
			typechart["Type3"]+=1
			print "same college"
		elif len(company_intercept)!=0:
			typechart["Type4"]+=1
		elif len(company_intercept) and len(highschool)!=0:
			typechart["Type5"]+=1
		elif len(company_intercept) and len(college)!=0:
			typechart["Type6"]+=1
		elif len(highschool) != 0 and len(college) !=0:
			typechart["Type7"]+=1
		elif len(highschool) != 0 and len(college) !=0 and len(company_intercept):
			typechart["Type8"]+=1
		else:
			typechart["Type1"]+=1

		#reset the connectionschool and connectioncompany
		connectionschool = {"College":[],"High School":[],"Graduate School":[]}
		connectioncompany = []
	return typechart

query = neo4j.CypherQuery(graph_db, "START n = node(*) WHERE has(n.nodeType) AND n.nodeType = 0 RETURN n LIMIT 35;")

for record in query.stream():
	node = record[0]
	typeCount(typechart,node)

print typechart



# print typeCount(typechart,12)
end_time = time.time()
print("Elapsed time was %g seconds" % (end_time - start_time))
# Connected + Nothing else = Type 1
# Connected + high-school = Type 2
# Connected + college-mate = Type 3
# Connected + co-worker = Type 4
# Connected + co-worker + high-school = Type 5
# Connected + co-worker + college mate = Type 6
# Connected + high-school + college mate = Type 7
# Connected + high school + college mate +co-worker = Type 8







#find particular nodes' high school and college
# node = graph_db.node(727)
# school = {"College":[],"High School":[]}
# schools = list(node.match_outgoing(rel_type = "STUDIED", end_node = None))
# for i in xrange(0,len(schools)):
# 	# print schools[i]._id
# 	rel = graph_db.relationship(schools[i]._id)
# 	# print rel.get_properties()["educationType"]
# 	school[rel.get_properties()["educationType"]].append(schools[i].end_node.get_properties()["name"])

# print school

# company = []
# #WORKED_AT
# companies = list(node.match_outgoing(rel_type = "WORKED_AT", end_node = None))
# for i in xrange(0,len(companies)):
# 	rel = graph_db.relationship(companies[i]._id)
# 	company.append(companies[i].end_node.get_properties()["name"])

# #WORKS_AT
# companies = list(node.match_outgoing(rel_type = "WORKS_AT", end_node = None))
# for i in xrange(0,len(companies)):
# 	# print schools[i]._id
# 	rel = graph_db.relationship(companies[i]._id)
# 	# print rel.get_properties()["educationType"]
# 	company.append(companies[i].end_node.get_properties()["name"])
# print company

# #CONNECTED NODES
# connections = list(node.match_outgoing(rel_type = "CONNECTED", end_node = None))
# connectionschool = {"College":[],"High School":[]}
# connectioncompany = []

# print connections[0].end_node
# #works_at
# connectioncompanies = list(connections[0].end_node.match_outgoing(rel_type = "WORKS_AT", end_node = None))
# print connectioncompanies

# for i in xrange(0,len(connectioncompanies)):
# 	# print schools[i]._id
# 	rel = graph_db.relationship(connectioncompanies[i]._id)
# 	# print rel.get_properties()["educationType"]
# 	connectioncompany.append(connectioncompanies[i].end_node.get_properties()["name"])

# print "this is connection company:"
# print connectioncompany
# #worked_at
# connectioncompanies = list(connections[0].end_node.match_outgoing(rel_type = "WORKED_AT", end_node = None))
# print connectioncompanies

# for i in xrange(0,len(connectioncompanies)):
# 	# print schools[i]._id
# 	rel = graph_db.relationship(connectioncompanies[i]._id)
# 	# print rel.get_properties()["educationType"]
# 	connectioncompany.append(connectioncompanies[i].end_node.get_properties()["name"])

# connectionschools = list(connections[0].end_node.match_outgoing(rel_type = "STUDIED", end_node = None))
# for i in xrange(0,len(connectionschools)):
# 	# print schools[i]._id
# 	rel = graph_db.relationship(connectionschools[i]._id)
# 	# print rel.get_properties()["educationType"]
# 	connectionschool[rel.get_properties()["educationType"]].append(connectionschools[i].end_node.get_properties()["name"])

# highschool = list(set(school["High School"]) & set(connectionschool["High School"]))
# college = list(set(school["College"]) & set(connectionschool["College"]))
# company_intercept = list(set(company) & set(connectioncompany))

# print highschool
# print college
# if len(highschool)!=0:
# 	typechart["Type2"]+=1
# 	print "same highschool"
# if len(college)!=0:
# 	typechart["Type3"]+=1
# 	print "same college"
# if len(highschool) != 0 and len(college) !=0:
# 	typechart["Type7"]+=1
# 	print "same highschool and college"

# print typechart
#print list(set(school["High School"]) & set(connectionschool["High School"]))
#print list(set(school["College"]) & set(connectionschool["College"]))


# print node.get_properties()



# node 12 check all his connection type
# node = graph_db.node(12)
# human = graph_db.get_indexed_relationship("userIdIndex","CONNECTED","12")
# connections = list(node.match_outgoing(rel_type = "CONNECTED", end_node = None))
# connections = list(node.match_outgoing(rel_type = "STUDIED", end_node = None))
# print connections[0]._id

# b = graph_db.relationship(connections[0]._id)
# b.get_properties()["educationType"]
# print b.get_properties()
# name = connections[0].end_node
# print name.get_properties()["name"]
# print(type(connections))



# url = "http://localhost:8001/db/data/relationship/122969794"
# new = neo4j.Node(url)
# print new.get_properties()
# b = graph_db.relationship(122969794)
# print b.get_properties()
# human.get_indexes(neo4j.Relationship)
# print human
# b = graph_db.relationship(122969750)
# print b.get_properties()["educationType"]