#!/usr/bin/python

from py2neo import neo4j
import MasterRelations
import Utilities

# Open database connection
CONNECTION_STR = "http://localhost:8001/db/data/"
graph_db = neo4j.GraphDatabaseService(CONNECTION_STR)

# jeff's imaginary friend = 74836374
# popular guy = 57584
# popular guy's type 8 friends: 

start_node = graph_db.node(57584)
end_node = graph_db.node(3090812)

relation = MasterRelations.get_relation(start_node, end_node, graph_db)

print str(relation)

