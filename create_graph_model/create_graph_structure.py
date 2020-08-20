import argparse
parser = argparse.ArgumentParser(description="create_graph_model_parser")
parser.add_argument("--home_directory", action="store", dest="home_directory", type=str, help="location of the home directory")
parser.add_argument("--geo_directory", action="store", dest="geo_directory", type=str, help="location of the geo shapefiles")
parser.add_argument("--data_directory", action="store", dest="data_directory", type=str, help="location of the relevant data directory")
parser.add_argument("--graph_directory", action="store", dest="graph_directory", type=str, help="location of the graph models")
parser.add_argument("--graph_model_name", action="store", dest="graph_model_name", type=str, help="name of new graph model")
parser.add_argument("--file_name", action="store", dest="file_name", type=str, help="name of the file we're updating")
parser.add_argument("--aggregate_function", action="store", dest="aggregate_function", type=str, help="aggregating function")
parser.add_argument("--aggregate_by", action="store", dest="aggregate_by", type=str, help="feature to aggregate on")
parser.add_argument("--features", action="store", dest="features", nargs='+',type=str, help="list of features to aggregate")
parser.add_argument('--gcp', action='store_true', dest='gcp', help='affects whether to configure to running on the cloud')

parse_results = parser.parse_args()
home_directory = parse_results.home_directory
geo_directory = parse_results.geo_directory
data_directory = parse_results.data_directory
graph_directory = parse_results.graph_directory
graph_model_name = parse_results.graph_model_name
file_name = parse_results.file_name
aggregate_function = parse_results.aggregate_function
aggregate_by = parse_results.aggregate_by
features = parse_results.features
gcp = parse_results.gcp

import sys
sys.path.append(home_directory)

import pandas as pd
import numpy as np
import json
import re
import networkx as nx
from itertools import combinations, product
if gcp:
    import gcsfs
    from google.cloud import storage
    storage_client = storage.Client()
import os

import utilities
import pynx_to_neo4j
import aggregate_features 


# use polygon json files as naming standard
if gcp:
    geo_bucket = storage_client.get_bucket(geo_directory[5:])
    blob = geo_bucket.blob('chicago_{}_reformatted.json'.format(aggregate_by))
    geo = json.loads(blob.download_as_string(client=None))

else:   
    with open('{}/chicago_{}_reformatted.json'.format(geo_directory, aggregate_by),'r') as f:
        geo = json.load(f)
   
df = pd.read_csv('{}/{}'.format(data_directory, file_name))

df_aggregated = \
aggregate_features.aggregate_features(df, aggregate_function, geo, aggregate_by, *features)

# determine whether a serialized version of this graph_model already exists 
exists = ""
if gcp:
    graph_bucket = storage_client.get_bucket(graph_directory[5:])
    graph_list = list(graph_bucket.list_blobs())
    result = [1 if graph_model_name in str(name) else 0 for name in graph_list]
    if sum(result) == 1:
        exists = True
        ix = result.index(1)
    else:
        exists = False

else:
    graph_list = os.listdir(graph_directory)
    result = [1 if graph_model_name in name else 0 for name in graph_list]
    if sum(result) == 1:
        exists = True
        ix = result.index(1)
    else:
        exists = False


if not exists:
    
    # create nodes named after 'aggregate_by' with property value attributes corresponding to df
    # e.g.: neighborhoods, chicago, residential
    G = pynx_to_neo4j.create_pynx_nodes(df_aggregated, node_category=aggregate_by, \
                                    attribute_columns=list(df_aggregated.columns))

    # create neighborhood to neighborhood edges
    G = pynx_to_neo4j.add_edges_to_pynx(G, "NEXT_TO", utilities.intersection, ["polygon_name_1", "polygon_name_2"], \
                                    aggregate_by ,bidirectional=True, polygon_dict_1=geo, \
                                    polygon_dict_2=geo)

if exists:

    print("adding to existing graph")
    if gcp:
        
        blob = graph_bucket.blob('{}.pkl'.format(graph_model_name))
        blob.download_to_filename("{}/create_graph_model/temp/temp.pkl".format(home_directory), client=None)
        G = nx.read_gpickle("{}/create_graph_model/temp/temp.pkl".format(home_directory))
    
    else:
        
        G = nx.read_gpickle("{}/{}.pkl".format(graph_directory, graph_model_name))

    # create nodes named after 'aggregate_by' with property value attributes corresponding to df
    # e.g.: neighborhoods, chicago, residential
    G = pynx_to_neo4j.create_pynx_nodes(df_aggregated, node_category=aggregate_by, \
                                    attribute_columns=list(df_aggregated.columns), existing_graph=G)

    # create neighborhood to neighborhood edges
    G = pynx_to_neo4j.add_edges_to_pynx(G, "NEXT_TO", utilities.intersection, ["polygon_name_1", "polygon_name_2"], \
                                    aggregate_by ,bidirectional=True, polygon_dict_1=geo, \
                                    polygon_dict_2=geo)



"""
RETURN TO THIS: SEPARATE SCRIPT FOR MULTIPLE GEO FILES
# create unidirectional edges between census tract and neighborhood
G = pynx_to_neo4j.add_edges_to_pynx(G, "CONTAINS", utilities.intersection, ["polygon_name_1", "polygon_name_2"], \
                                    "neighborhood", "tract", bidirectional=False, polygon_dict_1=neighborhoods, \
                                    polygon_dict_2=tracts)
G = pynx_to_neo4j.add_edges_to_pynx(G, "IS_WITHIN", utilities.intersection, ["polygon_name_1", "polygon_name_2"], \
                                    "tract", "neighborhood", bidirectional=False, polygon_dict_1=tracts, \
                                    polygon_dict_2=neighborhoods)

"""

# save as pkl file
if gcp:
    # write to disk first
    nx.write_gpickle(G, "temp/{}.pkl".format(graph_model_name))
    # then upload to bucket
    blob = graph_bucket.blob('{}.pkl'.format(graph_model_name))
    blob.upload_from_filename("temp/{}.pkl".format(graph_model_name))
    
else:
    nx.write_gpickle(G, "{}/{}.pkl".format(graph_directory, graph_model_name))



