import pandas as pd
import numpy as np
import json

def aggregate_features(features_dataframe, aggregate_function, geo_data_directory, *features_to_aggregate):
	
	"""
	Takes a a pandas dataframe of socioeconomic data (e.g. crime, property values)
	and returns a new frame aggregated by census tract and neighborhood
	:features_dataframe: socioeconomic features; must have 'tract' and 'neighborhood' column
	:aggregate_function: str, must be either 'count' or 'mean'
	:geo_data_directory: directory location of the geo shapefiles
	:*features_to_aggregate: args, series of features from the dataframe that you want aggregated
	Returns: dataframe with specified features aggregated by neighborhood and census tract
	"""

	assert(isinstance(features_dataframe, pd.core.frame.DataFrame)), "\
			features_dataframe must be a pandas dataframe"

	assert(aggregate_function == "count" or aggregate_function == "mean"), "\
			aggregate function must be either count or mean"
	# pull out unique aggregator categories
	with open('{}/neighborhood_polys.json'.format(geo_data_directory),'r') as f:
		neighborhoods = json.load(f)
	
	with open('{}/tract_polys.json'.format(geo_data_directory),'r') as f:
		tracts = json.load(f)

	unique_neighborhoods = list(neighborhoods.keys())
	unique_census_tracts = list(tracts.keys())
	# determine features to retain in dataframe
	features = list(features_to_aggregate) + ["neighborhood","tracts"]
	features_dataframe = features_dataframe[features]
			   
	if aggregate_function == "mean":
		
		# calculate aggregated figures for each neighborhood
		neighborhood_aggregated = features_dataframe.groupby('neighborhood')\
							 [list(features_to_aggregate)].mean()
		# determine whether
		additionals = list(set(unique_neighborhoods) - set(list(neighborhood_aggregated.index)))
		for i in additionals:
			if i != np.nan:
				neighborhood_aggregated.append(pd.Series(name=i))
		
		# calculate aggregated features for each census tract    
		census_tract_aggregated = features_dataframe.groupby('tracts')\
							 [list(features_to_aggregate)].mean()
		additionals = list(set(unique_census_tracts) - set(list(census_tract_aggregated.index)))
		for i in additionals:
			if i != np.nan:
				census_tract_aggregated.append(pd.Series(name=i))
		
	elif aggregate_function == "count":
		
		# calculate aggregated figures for each neighborhood
		neighborhood_aggregated = features_dataframe.groupby('neighborhood')\
							 [list(features_to_aggregate)].count()
		additionals = list(set(unique_neighborhoods) - set(list(neighborhood_aggregated.index)))
		for i in additionals:
			if i != np.nan:
				neighborhood_aggregated.append(pd.Series(name=i))
		
		# calculate aggregated features for each census tract    
		census_tract_aggregated = features_dataframe.groupby('tracts')\
							 [list(features_to_aggregate)].count()
		additionals = list(set(unique_census_tracts) - set(list(census_tract_aggregated.index)))
		for i in additionals:
			if i != np.nan:
				census_tract_aggregated.append(pd.Series(name=i))
		
	return neighborhood_aggregated, census_tract_aggregated
				
	
			
		
	
	
	
	
	
	


