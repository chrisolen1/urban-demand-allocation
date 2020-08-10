import sys
sys.path.append('/Users/chrisolen/Documents/uchicago_courses/optimization/project/urban-demand-allocation')
import pandas as pd
import numpy as np
import random
from scipy import spatial
from tqdm import tqdm
from py2neo import Graph
import utilities


def sample_addresses(addresses, localities, locality_type, sample_size=10000, seed=10):

	"""
	take a random sample from the address book and add locality metadata based on lat-long coordinates
	:addresses: address book csv file
	:localities: json geo shape file 
	:locality_type: type of localities you're generating metadata for (e.g. neighborhoods, tracts)
	:sample_size: the number of addresses to be random.y sampled
	Returns: csv of sampled addresses with locality metadata
	"""
	
	# filtering for just Chicago, dropping unnecessary columns
	addresses = addresses[addresses["PLACENAME"]=="Chicago"][["ADDRDELIV","LATITUDE","LONGITUDE"]]
	addresses = addresses.reset_index()
	addresses.drop(["index"], inplace=True, axis=1)
	random.seed(seed)
	# take a random sample of coordinates to reduce the number of variables of the optimization problem
	rand_index = random.sample(range(0, 582676), sample_size)
	# these will become our optimization variables
	address_sample = addresses.iloc[rand_index] 
	# zip coordinates for locality look up
	zipped_coords = list(zip(address_sample["LONGITUDE"],address_sample["LATITUDE"]))
	
	coord_locality = []
	
	print("assigning locality names to sampled address coordinates")
	for j in tqdm(range(len(zipped_coords))):
		result = utilities.point_lookup(localities, zipped_coords[j])
		coord_locality.append(result)
		
	address_sample["{}".format(locality_type)] = coord_locality
	
	return address_sample

def generate_distance_matrix(address_sample, demand_model):

	"""
	generate a matrix of euclidean distances between each sampled address
	and the addresses of the relevant stores
	:address_sample: csv output of sample_addresses function 
	:demand_model: csv output of build_demand_model function 
	Return: numpy matrix of element-wise euclidean distances
	"""

	# filter randomly sampled coordinates into ndarray
	address_lat = np.array(address_sample["LATITUDE"])
	address_long = np.array(address_sample["LONGITUDE"])
	address_coords = np.transpose(np.vstack((address_lat,address_long)))

	# filter store coordinates into ndarray
	demand_model_lat = np.array(demand_model["latitude"])
	demand_model_long = np.array(demand_model["longitude"])
	store_coords = np.transpose(np.vstack((demand_model_lat,demand_model_long)))
	
	# run them throw scipy's handy pairwise distance function
	distance_matrix = spatial.distance.cdist(store_coords, address_coords)
	
	return distance_matrix

def graph_to_address_frame(graph, address_frame, feature, localities, locality_type, edge_relation=None):
	
	"""
	:graph: neo4j object from py2neo
	:address_frame: pandas dataframe of sampled addresses
	:feature: str, location-based feature to be added from the neo4j graph to the address_frame (e.g. avg_property_value)
	:localities: json of locality shape coordinates of search area 
	:locality_type: str, locality type corresponding to the node types to which we're restricting the query (e.g. neighborhood or tract)
	Returns: updated address dataframe with new feature column
	"""
	
	# pull long_lat coordinates for each relevant address
	address_coordinates = list(zip(address_frame["LONGITUDE"],address_frame["LATITUDE"]))
	# create new column for feature; rename feature if edge relationship True
	if edge_relation:
		modified_feature = feature + "_" + edge_relation
		address_frame[modified_feature] = np.nan
	else:
		address_frame[feature] = np.nan
	# and empty list for those coordinates outside of the immediate search area 
	# (determined by the localities shapefiles)
	outside_search_area = []
	# iterate through lat_long pairs for each address
	if edge_relation:
		print("iterating through address_coordinates to build {} feature".format(modified_feature))
	else:
		print("iterating through address_coordinates to build {} feature".format(feature))
	
	for i in tqdm(range(len(address_coordinates))):

		# get location label based on localities shape file
		point_location = utilities.point_lookup(localities,address_coordinates[i])
		# pull out the feature associated with the locality that the address is located within
		try:
			if not edge_relation:

				
				result = float(dict(pd.DataFrame(graph.run('match (a:{}) \
												where a.name = "{}" return a'.format(locality_type,point_location)). \
												to_table()).iloc[0,0])[feature])
				## coordinates and df indices should be the same ## 
				address_frame[feature].iloc[i] = result
				
			else:
				
				result = pd.DataFrame(graph.run('match (a:{})-[:{}]->(b) \
												where a.name = "{}" \
												return b'.format(locality_type,edge_relation,point_location)). \
												to_table())
				# count number of edge relations returned
				n_edge_relations = len(result[0])
				edge_features = []
				# pull out each of the feature values for each of the edge relations 
				for j in range(n_edge_relations):
					edge_feature = float(dict(result[0][j])[feature])
					edge_features.append(edge_feature)
				# average over edge relation feature values, ignoring any NaNs
				mean_of_edge_features = np.nanmean(edge_features)
		
				## coordinates and df indices should be the same ## 
				demand[modified_feature].iloc[i] = mean_of_edge_features
		
		# it may be that the coordinates don't match any of the localities associated with the search area
		except:
			outside_search_area.append((i, address_coordinates[i]))
	 
	# for the coordinates that lie (usually barely) outside the search area 
	print("assigning values for coordinates just outside the area of interest")
	for i in tqdm(range(len(outside_search_area))): 
	
		point_location = utilities.closest_to(localities,outside_search_area[i][1])
	
		if not edge_relation:
				
			result = float(dict(pd.DataFrame(graph.run('match (a:{}) \
											where a.name = "{}" return a'.format(locality_type,point_location)). \
											to_table()).iloc[0,0])[feature])

			## coordinates and df indices should be the same ## 
			address_frame[feature].iloc[outside_search_area[i][0]] = result
				
		else:
				
			result = pd.DataFrame(graph.run('match (a:{})-[:{}]->(b) \
											where a.name = "{}" \
											return b'.format(locality_type,edge_relation,point_location)). \
											to_table())
			# count number of edge relations returned
			n_edge_relations = len(result[0])
			edge_features = []
			# pull out each of the feature values for each of the edge relations 
			for j in range(n_edge_relations):
				edge_feature = float(dict(result[0][j])[feature])
				edge_features.append(edge_feature)
			# average over edge relation feature values, ignoring any NaNs
			mean_of_edge_features = np.nanmean(edge_features)
		
			## coordinates and df indices should be the same ## 
			address_frame[modified_feature].iloc[outside_search_area[i][0]] = mean_of_edge_features
		
	return address_frame	







