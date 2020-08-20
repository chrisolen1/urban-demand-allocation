import argparse
parser = argparse.ArgumentParser(description="create_graph_model_parser")
parser.add_argument("--home_directory", action="store", dest="home_directory", type=str, help="location of the home directory")
parser.add_argument("--graph_directory", action="store", dest="graph_directory", type=str, help="location of the graph models")
parser.add_argument("--graph_model_name", action="store", dest="graph_model_name", type=str, help="name of new graph model")
parser.add_argument('--gcp', action='store_true', dest='gcp', help='affects whether to configure to running on the cloud')

parse_results = parser.parse_args()
home_directory = parse_results.home_directory
graph_directory = parse_results.graph_directory
graph_model_name = parse_results.graph_model_name
gcp = parse_results.gcp

import sys
sys.path.append(home_directory)

import networkx as nx
if gcp:
    import gcsfs
    from google.cloud import storage
    graph_bucket = storage_client.get_bucket(graph_directory[5:])

import pynx_to_neo4j

if gcp:

	blob = graph_bucket.blob('{}.pkl'.format(graph_model_name))
	G = nx.read_gpickle(blob.download_as_string(client=None))

else:

	G = nx.read_gpickle("{}/{}.pkl".format(graph_directory, graph_model_name))

# convert to neo4j query
neo = pynx_to_neo4j.pynx_to_neo4j_queries(G, return_nodes=True, return_edges=True)

# save as txt file
if gcp:
    graph_bucket = storage_client.get_bucket(graph_directory[5:])
    joined_neo = "\n".join(neo)
    bucket.blob('{}.txt'.format(graph_model_name)).upload_from_string(joined_neo, 'text/csv')

else:
    with open('{}/{}.txt'.format(graph_directory, graph_model_name), 'w') as neo_text:
        for listitem in neo:
            neo_text.write('%s\n' % listitem)
