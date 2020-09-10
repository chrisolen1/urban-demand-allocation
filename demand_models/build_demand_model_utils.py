import pandas as pd
import numpy as np
import json

import os
import subprocess
import multiprocessing as mp
from functools import partial

import re

from tqdm import tqdm

import utilities

def business_filter(bus_frame, naics_codes):

	"""
	:bus_frame: business dataframe
	:years: list of integer years you would like to select out
	:naics_codes: list of string naics codes you would like to select out,
				will match only up to the length of the code provided
	Returns: filtered business dataframe, potentially including sales volume for 
	the same store for multiple years 
	"""
	
	assert(isinstance(naics_codes,list)), "\
		naics_code argument must be of type list"
	
	assert(all(element for element in [isinstance(i,str) for i in naics_codes])), "\
		all naics_codes must be of type str"

	
	# filter for indicated years and naics_codes
	bus = naics_filter(bus_frame, naics_codes)
	# drop meta data and other info used for filtering
	bus.drop(['abi','primary_naics_code','company','business_status_code','company_holding_status',
		'year_established','employee_size_location'], axis=1, inplace=True)

	return bus
		

def naics_filter(bus_frame, naics_codes):
	
	"""
	:bus_frame: business dataframe
	:years: list of integer years you would like to select out
	:naics_codes: list of string naics codes you would like to select out,
				will match only up to the length of the code provided
	Returns: dataframe of business data filtered for year and naics code
	"""
	
	return bus_frame[bus_frame['primary_naics_code'].apply(parse_naics, args=[naics_codes])]
	
	
def parse_naics(df_value, naics):
	
	"""
	filter provided dataframe for naics codes. 
	mean to be used in df.apply() 
	"""

	results = []
	for i in naics:
		
		naics_length = len(i)
		truncated_naics = df_value[:naics_length]
		if truncated_naics == i:
			results.append(True)
		else:
			results.append(False)
			
	return any(results)

def graph_to_demand_model(demand_frame, geo_directory, geo_entity, city, edge_relation=None):
	
	"""
	:graph: neo4j object from py2neo
	:demand_frame: pandas dataframe of features predicting demand (sales) for each relevant business
	:feature: str, location-based feature to be added from the neo4j graph to the demand_frame (e.g. avg_property_value)
	:localities: json of locality shape coordinates of search area 
	:geo_entity: str, locality type corresponding to the node types to which we're restricting the query (e.g. neighborhood or tract)
	Returns: updated demand dataframe with new feature column
	"""
	
	
	print("populating demand predictors")
	
	inside_search_area = demand_frame[demand_frame[geo_entity] != "None"]
	outside_search_area = demand_frame[demand_frame[geo_entity] == "None"]
	
	if not edge_relation:

		geo_tags = inside_search_area[geo_entity].unique()		
		nq = partial(neo_query, geo_entity=geo_entity, edge_relation=None)
		results_list = run_imap_multiprocessing(func=nq, argument_list=geo_tags, num_processes=os.cpu_count(), chunksize=1)
		os.system("stty sane")
		query_string = 'match (a:{}) where a.name = "{}" return a'.format(geo_entity,geo_tags[0].replace("'","").replace(",",""))	
		output = str(subprocess.check_output("kubectl exec -it neo4j-ce-1-0 \
					-- cypher-shell -u 'neo4j' -p 'asdf' -d 'neo4j' --format plain '{}'".format(query_string), shell=True))
		output = re.findall("(\{[\W\w]+\})",output)[0].replace("{","").replace("}","").split(",")		
		output = [j.rstrip().lstrip().split(":") for j in output]
		features = []
		for k in output:
			features.append(geo_entity + "_" + k[0])
		features[0] = geo_entity
		geo_tag_df = pd.DataFrame(results_list, columns = features)
		
		inside_search_area = inside_search_area.merge(geo_tag_df, how="left", on=geo_entity)

	else:
				
		geo_tags = inside_search_area[geo_entity].unique()							
		nq = partial(neo_query, geo_entity=geo_entity, edge_relation=edge_relation)
		results_list = run_imap_multiprocessing(func=nq, argument_list=geo_tags, num_processes=os.cpu_count(), chunksize=1)
		os.system("stty sane")
		query_string = 'match (a:{}) where a.name = "{}" return a'.format(geo_entity,geo_tags[0].replace("'","").replace(",",""))	
		output = str(subprocess.check_output("kubectl exec -it neo4j-ce-1-0 \
					-- cypher-shell -u 'neo4j' -p 'asdf' -d 'neo4j' --format plain '{}'".format(query_string), shell=True))
		output = re.findall("(\{[\W\w]+\})",output)[0].replace("{","").replace("}","").split(",")		
		output = [j.rstrip().lstrip().split(":") for j in output]
		features = []
		for k in output:
			features.append(k[0])
		features[0] = geo_entity
		modified_features = []
		for i in range(1,len(features)):
			modified_features.append(geo_entity + "_" + features[i] + "_" + edge_relation)
		features += modified_features	

		geo_tag_df = pd.DataFrame(results_list, columns = features)

		inside_search_area = inside_search_area.merge(geo_tag_df, how="left", on=geo_entity)

	if outside_search_area.shape[0] != 0:
		print("a few of the business fall a bit outside {}. associating them with their closest {}...".format(city,geo_entity))
		# pulling neighborhood polygons
		with open('{}/{}_{}_reformatted.json'.format(geo_directory, city, geo_entity),'r') as f:
			localities = json.load(f)

		for i in range(1,len(features)):
			outside_search_area[features[i]] = np.nan


		if not edge_relation:
			
			for i in tqdm(range(len(outside_search_area))): 
	
				geo_tag = utilities.closest_to(localities,tuple(outside_search_area.iloc[i][['latitude','longitude']]))
				result = neo_query(geo_tag, geo_entity, edge_relation=None)
				for j in range(len(features)):
					outside_search_area.iloc[i][features[j]] = result[j]

			pieces = (outside_search_area,inside_search_area)
			demand_frame = pd.concat(pieces, ignore_index=True)
			return demand_frame

		else:

			for i in tqdm(range(len(outside_search_area))): 

				geo_tag = utilities.closest_to(localities,tuple(outside_search_area.iloc[i][['latitude','longitude']]))
				result = neo_query(geo_tag, geo_entity, edge_relation=edge_relation)
				for j in range(len(features)):
					outside_search_area.iloc[i][features[j]] = result[j]

			pieces = (outside_search_area,inside_search_area)
			demand_frame = pd.concat(pieces, ignore_index=True)	
			return demand_frame

	return inside_search_area	

def run_imap_multiprocessing(func, argument_list, num_processes, chunksize=10):

		
		pool = mp.Pool(processes=num_processes)
		result_list = []
		for result in tqdm(pool.imap(func=func, iterable=argument_list, chunksize=chunksize), total=len(argument_list)):
			
			result_list.append(result)

		return result_list

def neo_query(geo_tag, geo_entity, edge_relation=None):		

	query_string = 'match (a:{}) where a.name = "{}" return a'.format(geo_entity,geo_tag.replace("'","").replace(",",""))	
	output = str(subprocess.check_output("kubectl exec -it neo4j-ce-1-0 \
					-- cypher-shell -u 'neo4j' -p 'asdf' -d 'neo4j' --format plain '{}'".format(query_string), shell=True))
		
	output = re.findall("(\{[\W\w]+\})",output)[0].replace("{","").replace("}","").replace('"','').split(",")		
	output = [j.rstrip().lstrip().split(":") for j in output]
		
	results = []
		
	for k in output:
		results.append(k[1].rstrip().lstrip())
		
	if edge_relation:
		
		query_string = 'match (a:{})-[:{}]->(b) where a.name = "{}" return b'.format(geo_entity,edge_relation,geo_tag.replace("'","").replace(",",""))			
		output = str(subprocess.check_output("kubectl exec -it neo4j-ce-1-0 \
					-- cypher-shell -u 'neo4j' -p 'asdf' -d 'neo4j' --format plain '{}'".format(query_string), shell=True))
		output = re.findall("{[\w\W]+?}",output)
		outputs = []
		for i in output:
			outputs.append(i.replace("{","").replace("}","").replace('"','').split(","))
		num_features = len(outputs[0])
		outputs = [[outputs[j][i] for j in range(len(outputs))] for i in range(1,num_features)]
		outputs = [[j.rstrip().lstrip().split(":") for j in sublist] for sublist in outputs]
		outputs = [[float([j][0][1].lstrip().rstrip()) for j in i] for i in outputs]
		outputs = [np.nanmean(np.array(i)) for i in outputs]
		results += outputs

	return results
	
def connect_to_neo4j(uri, username, password):

	"""
	establish connection to neo4j
	:uri: uri and port neo4j server is listening on 
	:username: username for neo4j server
	:password: password for neo4j server
	Return: neo4j instance
	"""

	from py2neo import Graph
	graph = Graph(uri, auth=(username, password))
	return graph

def _local_graph_to_demand_model(graph, demand_frame, feature, localities, geo_entity, edge_relation=None):
	
	"""
	:graph: neo4j object from py2neo
	:demand_frame: pandas dataframe of features predicting demand (sales) for each relevant business
	:feature: str, location-based feature to be added from the neo4j graph to the demand_frame (e.g. avg_property_value)
	:localities: json of locality shape coordinates of search area 
	:geo_entity: str, locality type corresponding to the node types to which we're restricting the query (e.g. neighborhood or tract)
	Returns: updated demand dataframe with new feature column
	"""
	
	# pull long_lat coordinates for each relevant business
	business_coordinates = list(zip(demand_frame["longitude"],demand_frame["latitude"]))
	# create new column for feature; rename feature if edge relationship True
	if edge_relation:
		modified_feature = feature + "_" + edge_relation
		demand_frame[modified_feature] = np.nan
	else:
		demand_frame[feature] = np.nan
	# and empty list for those coordinates outside of the immediate search area 
	# (determined by the localities shapefiles)
	outside_search_area = []
	# iterate through lat_long pairs for each business
	for i in range(len(business_coordinates)):

		# get location label based on localities shape file
		point_location = utilities.point_lookup(localities,business_coordinates[i])
		# pull out the feature associated with the locality that the business is located within
		try:
			if not edge_relation:

				
				result = float(dict(pd.DataFrame(graph.run('match (a:{}) \
												where a.name = "{}" return a'.format(geo_entity,point_location)). \
												to_table()).iloc[0,0])[feature])
				## coordinates and df indices should be the same ## 
				demand_frame[feature].iloc[i] = result
				
			else:
				
				result = pd.DataFrame(graph.run('match (a:{})-[:{}]->(b) \
												where a.name = "{}" \
												return b'.format(geo_entity,edge_relation,point_location)). \
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
			outside_search_area.append((i, business_coordinates[i]))
	 
	# for the coordinates that lie (usually barely) outside the search area 
	for i in range(len(outside_search_area)): 
	
		point_location = utilities.closest_to(localities,outside_search_area[i][1])
	
		if not edge_relation:
				
			result = float(dict(pd.DataFrame(graph.run('match (a:{}) \
											where a.name = "{}" return a'.format(geo_entity,point_location)). \
											to_table()).iloc[0,0])[feature])

			## coordinates and df indices should be the same ## 
			demand_frame[feature].iloc[outside_search_area[i][0]] = result
				
		else:
				
			result = pd.DataFrame(graph.run('match (a:{})-[:{}]->(b) \
											where a.name = "{}" \
											return b'.format(geo_entity,edge_relation,point_location)). \
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
			demand_frame[modified_feature].iloc[outside_search_area[i][0]] = mean_of_edge_features
		
	return demand_frame	
	

	