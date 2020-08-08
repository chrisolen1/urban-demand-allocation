import sys
sys.path.append('/Users/chrisolen/Documents/uchicago_courses/optimization/project/urban-demand-allocation')

import pandas as pd
import numpy as np
import json

from py2neo import Graph

from sklearn.linear_model import LinearRegression

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
years = [2016,2017,2015]
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

demand.to_csv("demand_model.csv", index=False)