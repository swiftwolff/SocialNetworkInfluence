#!/usr/bin/python

from py2neo import neo4j
import time
import logging

import MasterRelations
import Utilities

start_time = time.time()

# Open database connection
CONNECTION_STR = "http://localhost:8001/db/data/"
#CONNECTION_STR = "http://localhost:7474/db/data/"
graph_db = neo4j.GraphDatabaseService(CONNECTION_STR)

# Set up logging to file - see previous section for more details
logging.basicConfig(level = logging.ERROR,
                    format = '%(message)s',
                    datefmt = '%m-%d %H:%M',
                    filename = "./outputs/output.log",
                    filemode = 'w')

console = logging.StreamHandler()
console.setLevel(logging.ERROR)
formatter = logging.Formatter('%(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console) # Add the handler to the root logger

start = 0
offset = 0
skip = 100
limit = 300
final_limit = 300

for x in range(start, final_limit, skip):
    
    # Query
    query = neo4j.CypherQuery(graph_db, "START r=rel(*) MATCH (n) -[r: CONNECTED]-> (m) WHERE n.nodeType = 0 AND m.nodeType = 0 RETURN n, m SKIP " + str(offset) + " LIMIT " + str(limit) + " ")

    start_node = None
    end_node = None

    for record in query.stream():

        start_node = record[0]
        end_node = record[1]

        relation = MasterRelations.get_relation(start_node, end_node, graph_db)    
        logging.error(str(start_node.get_properties()['id']) + ', ' + str(end_node.get_properties()['id']) + ", " + str(relation))

    offset += skip    
    limit += skip

    if x == final_limit - skip:
        end_time = time.time()
        elapsed_time = Utilities.elapsed_time(start_time, end_time)

        logging.error(elapsed_time[0])
        logging.error(elapsed_time[1])
        logging.error(elapsed_time[2])

