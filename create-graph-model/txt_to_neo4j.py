import argparse
parser = argparse.ArgumentParser(description="txt_to_neo4j_parser")
parser.add_argument("--graph_directory", action="store", dest="graph_directory", type=str, help="location of the graph models")
parser.add_argument("--graph_model_name", action="store", dest="graph_model_name", type=str, help="name of new graph model")
parser.add_argument('--gcp', action='store_true', dest='gcp', help='affects whether to configure to running on the cloud')

parse_results = parser.parse_args()

graph_directory = parse_results.graph_directory
graph_model_name = parse_results.graph_model_name
gcp = parse_results.gcp

"""
set up neo4j graph using queries generated from 
pynx_to_neo4j.py
"""

from py2neo import Graph

if gcp:

	import gcsfs
    from google.cloud import storage
    graph_bucket = storage_client.get_bucket(graph_directory[5:])
    blob = graph_bucket.blob('{}.pkl'.format(graph_model_name))
    neo = blob.download_as_string().decode("utf-8").replace('\n', ' \n')

    # Create a node via cypher-shell
	#kubectl exec -it "${APP_INSTANCE_NAME}-0" --namespace "${NAMESPACE}" -- cypher-shell -u "neo4j" -p "${NEO4J_PASSWORD}" -d "neo4j" 'CREATE(n:Person { name: "John Doe"})'


else:

	with open('{}/{}.txt'.format(graph_directory, graph_model_name), 'r') as file:
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
