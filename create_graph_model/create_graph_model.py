import sys
sys.path.append('/Users/chrisolen/Documents/uchicago_courses/optimization/project/urban-demand-allocation')

import pandas as pd
import numpy as np
import json
import re
import networkx as nx
from itertools import combinations, product

import utilities
import pynx_to_neo4j
import aggregate_features 

with open('../../data/geo_shape_files/neighborhood_reformatted.json','r') as f:
    neighborhoods = json.load(f)
    
with open('../../data/geo_shape_files/tract_reformatted.json','r') as f:
    tracts = json.load(f)    

# produce aggregated figures for property values
properties = pd.read_csv("../../data/residential_standardized.csv")
properties_neighborhood_aggregated, properties_tract_aggregated = \
aggregate_features.aggregate_features(properties, "mean", "../../data/geo_shape_files", "zestimate", "lotSize")

# produce aggregated figures for crime
crime = pd.read_csv("../../data/crime_standardized.csv")
crime_neighborhood_aggregated, crime_tract_aggregated = \
aggregate_features.aggregate_features(crime, "count", "../../data/geo_shape_files", "primary_type")

# create neighborhood nodes with property value attributes
G = pynx_to_neo4j.create_pynx_nodes(properties_neighborhood_aggregated, node_category='neighborhood', \
                                    attribute_columns=list(properties_neighborhood_aggregated.columns))
# create neighborhood nodes with crime attributes
G = pynx_to_neo4j.create_pynx_nodes(crime_neighborhood_aggregated,node_category='neighborhood', \
                                    attribute_columns=list(crime_neighborhood_aggregated.columns), \
                                    existing_graph=G)
# create neighborhood to neighborhood edges
G = pynx_to_neo4j.add_edges_to_pynx(G, "NEXT_TO", utilities.intersection, ["polygon_name_1", "polygon_name_2"], \
                                    "neighborhood",bidirectional=True, polygon_dict_1=neighborhoods, \
                                    polygon_dict_2=neighborhoods)

# create tract nodes with property value attributes
G = pynx_to_neo4j.create_pynx_nodes(properties_tract_aggregated, node_category='tract', \
                                    attribute_columns=list(properties_tract_aggregated.columns), existing_graph=G)
# create census tract to census tract edges
G = pynx_to_neo4j.add_edges_to_pynx(G, "NEXT_TO", utilities.intersection, ["polygon_name_1", "polygon_name_2"], \
                                    "tract", bidirectional=True, polygon_dict_1=tracts, \
                                    polygon_dict_2=tracts)

# create unidirectional edges between census tract and neighborhood
G = pynx_to_neo4j.add_edges_to_pynx(G, "CONTAINS", utilities.intersection, ["polygon_name_1", "polygon_name_2"], \
                                    "neighborhood", "tract", bidirectional=False, polygon_dict_1=neighborhoods, \
                                    polygon_dict_2=tracts)
G = pynx_to_neo4j.add_edges_to_pynx(G, "IS_WITHIN", utilities.intersection, ["polygon_name_1", "polygon_name_2"], \
                                    "tract", "neighborhood", bidirectional=False, polygon_dict_1=tracts, \
                                    polygon_dict_2=neighborhoods)

# convert to neo4j query
neo = pynx_to_neo4j.pynx_to_neo4j_queries(G, return_nodes=True, return_edges=True)

# save as txt file
with open('../../data/graph_models/neo_queries.txt', 'w') as neo_text:
    for listitem in neo:
        neo_text.write('%s\n' % listitem)
