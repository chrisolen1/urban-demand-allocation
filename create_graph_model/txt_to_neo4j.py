"""
set up neo4j graph using queries generated from 
pynx_to_neo4j.py
"""

from py2neo import Graph

with open('graph_models/neo.txt', 'r') as file:
    neo = file.read().replace('\n', ' \n')

# establish connection to neo4j
graph = Graph("bolt://localhost:7687", user="neo4j", password="password")
print("connected to neo4j server")

# delete existing graph if one exists
trans_action = graph.begin()
statement = "MATCH (n) DETACH DELETE n"
trans_action.run(statement)
trans_action.commit()
print("existing schema deleted")

# run queries
trans_action = graph.begin()
statement = neo
trans_action.run(neo)
trans_action.commit()
print("new schema created")