import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--home_directory", action="store", dest="home_directory", type=str, help="location of the home directory")
parser.add_argument("--geo_directory", action="store", dest="geo_directory", type=str, help="location of the geo shapefiles")
parser.add_argument("--data_directory", action="store", dest="data_directory", type=str, help="location of the relevant data directory")
parser.add_argument("--graph_directory", action="store", dest="graph_directory", type=str, help="location of the graph models")
parser.add_argument('--gcp', action='store_true', dest='gcp', help='affects whether to configure to running on the cloud')

parse_results = parser.parse_args()
home_directory = parse_results.home_directory
geo_directory = parse_results.geo_directory
data_directory = parse_results.data_directory
graph_directory = parse_results.graph_directory
gcp = parse_results.gcp

import sys
sys.path.append(home_directory)

import pandas as pd
import numpy as np
import json

from py2neo import Graph

import utilities
from demand_models.build_demand_model_utils import business_filter, connect_to_neo4j, graph_to_demand_model

# load in raw business data 
df_types = pd.read_csv('../../data/dtypes.csv')['dtypes']
bus = pd.read_csv("../../data/chi_bus_cleaned.csv",dtype=df_types.to_dict())

# pulling neighborhood polygons
with open('../../data/geo_shape_files/neighborhood_reformatted.json','r') as f:
    neighborhoods = json.load(f)
    
# pulling tract polygons
with open('../../data/geo_shape_files/tract_reformatted.json','r') as f:
    tracts = json.load(f)

# specify years and naics codes of interest     
years = [2017] # at the moment, we should just keep it to one year since socioecon data is just one year 
years.sort(reverse=True)
naics = ['445110','335']   

# specify neo4j server information 
uri = "bolt://localhost:7687"
username = 'neo4j'
password = "password"

# filter business data for year and naics codes
demand = business_filter(bus, years, naics)

# connect to neo4j server
graph = connect_to_neo4j(uri, username, password)

# create location-oriented socioeconomic features
demand = graph_to_demand_model(graph, demand, "zestimate", neighborhoods, "neighborhood")
demand = graph_to_demand_model(graph, demand, "primary_type", neighborhoods, "neighborhood")
demand = graph_to_demand_model(graph, demand, "zestimate", neighborhoods, "neighborhood", edge_relation="NEXT_TO")
demand = graph_to_demand_model(graph, demand, "primary_type", neighborhoods, "neighborhood", edge_relation="NEXT_TO")

# remove any null values
demand.dropna(inplace=True)

demand.to_csv("../../data/demand_model.csv", index=False)

