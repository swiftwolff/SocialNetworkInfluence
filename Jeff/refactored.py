from py2neo import neo4j
import time
import smtplib

start_time=time.time()
# user_name => 'biddies.help@gmail.com'
# password => 'biddiespassword'

#Python Email Client
SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587

sender = 'biddies.help@gmail.com'
password = 'biddiespassword'
recipient = 'jeff.hsu@west.cmu.edu'
subject = 'Type Script Result - Research Project'

uri = "http://localhost:8001/db/data/"
graph_db = neo4j.GraphDatabaseService(uri)
typechart = {"Type1":0,"Type2":0,"Type3":0,"Type4":0,"Type5":0,"Type6":0,"Type7":0,"Type8":0}

def getNodeSchools(node):
	school = {"College":[],"High School":[],"Graduate School":[]}
	schools = list(node.match_outgoing(rel_type = "STUDIED", end_node = None))
	for i in xrange(0,len(schools)):
		rel = graph_db.relationship(schools[i]._id)
		school[rel.get_properties()["educationType"]].append(schools[i].end_node.get_properties()["name"])
	return school

def getNodeCompanies(node):
	company = []
	#WORKED_AT
	companies = list(node.match_outgoing(rel_type = "WORKED_AT", end_node = None))
	for i in xrange(0,len(companies)):
		company.append(companies[i].end_node.get_properties()["name"])

	#WORKS_AT
	companies = list(node.match_outgoing(rel_type = "WORKS_AT", end_node = None))
	for i in xrange(0,len(companies)):
		company.append(companies[i].end_node.get_properties()["name"])

	return company

def getNodeConnections(node):
	connections = list(node.match_outgoing(rel_type = "CONNECTED", end_node = None)) + \
	list(node.match_incoming(rel_type = "CONNECTED"))
	return connections

def getConnectionSchool(connection,node):
	connectionschool={"College":[],"High School":[],"Graduate School":[]}
	if connection.end_node == node:
		connectionschools = list(connection.start_node.match_outgoing(rel_type = "STUDIED", end_node = None))
	else:
		connectionschools = list(connection.end_node.match_outgoing(rel_type = "STUDIED", end_node = None))
	# print connectionschools

	for l in xrange(0,len(connectionschools)):
		rel = graph_db.relationship(connectionschools[l]._id)
		try:
			# print rel.get_properties()["educationType"]
			connectionschool[rel.get_properties()["educationType"]].append(connectionschools[l].end_node.get_properties()["name"])
		except:
			continue
	return connectionschool

def getConnectionCompany(connection,node):
	connectioncompany = []
	if connection.end_node == node:
		# print "node and end node are the same!"
		connectioncompanies = list(connection.start_node.match_outgoing(rel_type = "WORKS_AT", end_node = None))
		for i in list(connection.start_node.match_outgoing(rel_type = "WORKED_AT", end_node = None)):
			connectioncompanies.append(i)
	else:
		connectioncompanies = list(connection.end_node.match_outgoing(rel_type = "WORKS_AT", end_node = None))
		for i in list(connection.end_node.match_outgoing(rel_type = "WORKED_AT", end_node = None)):
			connectioncompanies.append(i)
		# print "This is connection companies"
		# print len(connectioncompanies)
		#works_at
	for j in xrange(0,len(connectioncompanies)):
		connectioncompany.append(connectioncompanies[j].end_node.get_properties()["name"])
	
	return connectioncompany

def typeCount(typechart,school,connectionschool,company,connectioncompany):
	highschool = list(set(school["High School"]) & set(connectionschool["High School"]))
	college = list(set(school["College"]) & set(connectionschool["College"]) or set(connectionschool["Graduate School"]))
	company_intercept = list(set(company) & set(connectioncompany))
	# highschool = ["hi"]
	# college = []
	# company_intercept=["hi"]
	# print "Here are the interceptions:"
	# print highschool
	# print college
	# print company_intercept

	if len(highschool)!=0 and len(college)!=0 and len(company_intercept)!=0:
		typechart["Type2"]+=1
		typechart["Type3"]+=1
		typechart["Type4"]+=1
		typechart["Type5"]+=1
		typechart["Type6"]+=1
		typechart["Type7"]+=1
		typechart["Type8"]+=1
	elif len(highschool)!=0 and len(college)!=0:
		typechart["Type2"]+=1
		typechart["Type3"]+=1
		typechart["Type7"]+=1
	elif len(highschool)!=0 and len(company_intercept)!=0:
		typechart["Type2"]+=1
		typechart["Type4"]+=1
		typechart["Type5"]+=1
	elif len(college)!=0 and len(company_intercept)!=0:
		typechart["Type3"]+=1
		typechart["Type4"]+=1
		typechart["Type6"]+=1
	elif len(highschool)!=0:
		typechart["Type2"]+=1
	elif len(college)!=0:
		typechart["Type3"]+=1
	elif len(company_intercept)!=0:
		typechart["Type4"]+=1
	else:
		typechart["Type1"]+=1
	return


# def getHumanNode():
# 	query = neo4j.CypherQuery(graph_db, "START n = node(*) WHERE has(n.nodeType) AND n.nodeType = 0 RETURN n LIMIT 10;")
# 	for record in query.stream():
# 		node = record[0]

#main execution script
query = neo4j.CypherQuery(graph_db, "START n = node(*) WHERE has(n.nodeType) AND n.nodeType = 0 RETURN n LIMIT 1000;")
for record in query.stream():
	node = record[0]
	schools = getNodeSchools(node)
	companies = getNodeCompanies(node)
	connections = getNodeConnections(node)
	for connection in connections:
		connectioncompanies = getConnectionCompany(connection,node)
		connectionschools = getConnectionSchool(connection,node)
		typeCount(typechart,schools,connectionschools,companies,connectioncompanies)
# for connection in connections:
# 	connectionschools = getConnectionSchool(connection,node)
# 	connectioncompanies = getConnectionCompany(connection,node)
# 	result = typeCount(typechart,schools,connectionschools,companies,connectioncompanies)

print typechart

# Connected + Nothing else = Type 1
# Connected + high-school = Type 2
# Connected + college-mate = Type 3
# Connected + co-worker = Type 4
# Connected + co-worker + high-school = Type 5
# Connected + co-worker + college mate = Type 6
# Connected + high-school + college mate = Type 7
# Connected + high school + college mate +co-worker = Type 8
end_time = time.time()
print("Elapsed time was %g seconds" % (end_time - start_time))

body = "Elapsed time was %g seconds" % (end_time - start_time)
 
"Sends an e-mail to the specified recipient."
 
body = body + " : " + str(typechart)
 
headers = ["From: " + sender,
           "Subject: " + subject,
           "To: " + recipient,
           "MIME-Version: 1.0",
           "Content-Type: text/html"]
headers = "\r\n".join(headers)
 
session = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
 
session.ehlo()
session.starttls()
session.ehlo
session.login(sender, password)
#session.sendmail(sender, recipient, headers + "\r\n\r\n" + body)
#session.quit()
