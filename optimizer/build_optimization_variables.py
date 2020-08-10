import sys
sys.path.append('/Users/chrisolen/Documents/uchicago_courses/optimization/project/urban-demand-allocation')

import pandas as pd
import numpy as np
import json

from py2neo import Graph

import utilities

from optimizer.optimization_var_utils import sample_addresses, generate_distance_matrix, graph_to_address_frame

# pull in store-level data from the demand model
demand_model = pd.read_csv("../../data/demand_model.csv")

# pull in all addresses
addresses = pd.read_csv("../../data/address_book.csv")

# pull in locality shapefiles
with open('../../data/geo_shape_files/neighborhood_reformatted.json','r') as f:
    neighborhoods = json.load(f)
    
# connect to graph db
uri = "bolt://localhost:7687"
graph = Graph(uri, auth=("neo4j", "password"))    

# load in betas from demand model
betas = pd.read_csv('../../data/optimization_variables/betas.csv', header = None).iloc[:,0].tolist()

# take a random sample from the address book 
address_sample = sample_addresses(addresses, neighborhoods, "neighborhood", sample_size=10000)

# update sample of addresses with socioeconomic data
address_matrix = graph_to_address_frame(graph, address_sample, "zestimate", neighborhoods, "neighborhood")
address_matrix = graph_to_address_frame(graph, address_matrix, "primary_type", neighborhoods, "neighborhood")
address_matrix = graph_to_address_frame(graph, address_matrix, "zestimate", neighborhoods, "neighborhood", edge_relation="NEXT_TO")
address_matrix = graph_to_address_frame(graph, address_matrix, "primary_type", neighborhoods, "neighborhood", edge_relation="NEXT_TO")

# remove any rows with nans
address_matrix.dropna(inplace=True)

# generate euclidean distances from each address to each store address
distance_matrix = generate_distance_matrix(address_matrix, demand_model)

# drop unnecessary features 
address_matrix.drop(['ADDRDELIV', 'LATITUDE', 'LONGITUDE', 'neighborhood'], axis=1, inplace=True)

# pull out most recdnt annual sales features
sales = demand_model['sales_volume_location']

# writing l2 norms to csv
distance_weighted_sales_norm = np.linalg.norm(np.matmul(np.transpose(sales), distance_matrix))
demand_beta_weighted_norm = np.linalg.norm(np.matmul(betas,np.transpose(np.array(address_matrix))))
dic = {"distance_weighted_sales_norm":[distance_weighted_sales_norm],"demand_beta_weighted_norm":[demand_beta_weighted_norm]}
norms = pd.DataFrame(dic)
norms.to_csv("../../data/optimization_variables/norms.csv", index=False)
print("norms written to file")

# writing final demand model to csv
demand_model.drop(['latitude','longitude'], axis=1, inplace=True)
demand_model = np.array(demand_model)
np.savetxt('../../data/optimization_variables/store_matrix.csv', demand_model, delimiter=",")
print("store matrix written to file")

# writing final address matrix to csv
address_matrix = np.array(address_matrix)
np.savetxt('../../data/optimization_variables/address_matrix.csv', address_matrix, delimiter=",")
print("address_matrix written to file")

# writing final distance matrix to csv
np.savetxt('../../data/optimization_variables/distance_matrix.csv', distance_matrix, delimiter=",")
print("distance matrix written to file")


