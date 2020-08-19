import pandas as pd
import numpy as np
import json

def aggregate_features(features_dataframe, aggregate_function, geo_data_directory, aggregate_by, *features_to_aggregate):
	
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
	
	assert(aggregate_by == "neighborhood" or aggregate_by == "tract"), "\
			aggregate_by must be either neighborhood or tract"


	unique_geos = list(geo.keys())
	# determine features to retain in dataframe
	features = list(features_to_aggregate).append(aggregate_by)
	features_dataframe = features_dataframe[features] 

	# remove rows from the socioeconomic data for which 'neighborhood' or 'tract' == 'None'
	# otherwise, you'll get a key error when trying to match up with the shapefiles later 
	if 'None' in list(features_dataframe[aggregate_by]):

		features_dataframe = features_dataframe[features_dataframe[aggregate_by] != 'None']
			   
	if aggregate_function == "mean":
		
		# calculate aggregated figures for each neighborhood
		aggregated = features_dataframe.groupby(aggregate_by)\
							 [list(features_to_aggregate)].mean()
		# determine whether there are geographic entities in the json files that are not in the
		# socioeconomic data
		additionals = list(set(unique_geos) - set(list(aggregated.index)))
		for i in additionals:
			if i != np.nan:
				aggregated = aggregated.append(pd.Series(name=i))
		
	elif aggregate_function == "count":
		
		# calculate aggregated figures for each neighborhood
		aggregated = features_dataframe.groupby(aggregate_by)\
							 [list(features_to_aggregate)].count()
		# determine whether there are geographic entities in the json files that are not in the
		# socioeconomic data
		additionals = list(set(unique_geos) - set(list(aggregated.index)))
		for i in additionals:
			if i != np.nan:
				aggregated = aggregated.append(pd.Series(name=i))
		
	return aggregated
				
	
			
		
	
	
	
	
	
	


