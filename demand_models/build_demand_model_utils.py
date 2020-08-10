import sys
sys.path.append('/Users/chrisolen/Documents/uchicago_courses/optimization/project/urban-demand-allocation')

import pandas as pd
import numpy as np

import utilities

def business_filter(bus_frame, years, naics_codes):

	"""
	:bus_frame: business dataframe
	:years: list of integer years you would like to select out
	:naics_codes: list of string naics codes you would like to select out,
				will match only up to the length of the code provided
	Returns: filtered business dataframe, potentially including sales volume for 
	the same store for multiple years 
	"""

	assert(isinstance(years,list)), "\
		years argument must be of type list"
	
	assert(all(element for element in [isinstance(i,int) for i in years])), "\
		all years must be of type int"
	
	assert(isinstance(naics_codes,list)), "\
		naics_code argument must be of type list"
	
	assert(all(element for element in [isinstance(i,str) for i in naics_codes])), "\
		all naics_codes must be of type str"

	# filter for indicated years and naics_codes
	bus = naics_year_filter(bus_frame, years, naics_codes)
	# drop meta data and other info used for filtering
	bus.drop(['abi','primary_naics_code','company','business_status_code','company_holding_status',
		'year_established','employee_size_location'], axis=1, inplace=True)

	return bus
		

def naics_year_filter(bus_frame, years, naics_codes):
	
	"""
	:bus_frame: business dataframe
	:years: list of integer years you would like to select out
	:naics_codes: list of string naics codes you would like to select out,
				will match only up to the length of the code provided
	Returns: dataframe of business data filtered for year and naics code
	"""
	
	bus_frame = bus_frame[bus_frame['primary_naics_code'].apply(parse_naics, args=[naics_codes])]
	
	return bus_frame[bus_frame['year'].isin(years)]
	
	
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

def graph_to_demand_model(graph, demand_frame, feature, localities, locality_type, edge_relation=None):
	
	"""
	:graph: neo4j object from py2neo
	:demand_frame: pandas dataframe of features predicting demand (sales) for each relevant business
	:feature: str, location-based feature to be added from the neo4j graph to the demand_frame (e.g. avg_property_value)
	:localities: json of locality shape coordinates of search area 
	:locality_type: str, locality type corresponding to the node types to which we're restricting the query (e.g. neighborhood or tract)
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
												where a.name = "{}" return a'.format(locality_type,point_location)). \
												to_table()).iloc[0,0])[feature])
				## coordinates and df indices should be the same ## 
				demand_frame[feature].iloc[i] = result
				
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
			outside_search_area.append((i, business_coordinates[i]))
	 
	# for the coordinates that lie (usually barely) outside the search area 
	for i in range(len(outside_search_area)): 
	
		point_location = utilities.closest_to(localities,outside_search_area[i][1])
	
		if not edge_relation:
				
			result = float(dict(pd.DataFrame(graph.run('match (a:{}) \
											where a.name = "{}" return a'.format(locality_type,point_location)). \
											to_table()).iloc[0,0])[feature])

			## coordinates and df indices should be the same ## 
			demand_frame[feature].iloc[outside_search_area[i][0]] = result
				
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
			demand_frame[modified_feature].iloc[outside_search_area[i][0]] = mean_of_edge_features
		
	return demand_frame	
	
	