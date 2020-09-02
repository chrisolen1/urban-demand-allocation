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
import os
import pandas as pd 
if gcp:
	import gcsfs
	from google.cloud import storage
	storage_client = storage.Client()

from graph_utils import graph_model

# set graph model name
graph_model_name = input("Provide the title of your graph: ")
gm = graph_model(home_directory=home_directory,
			geo_directory=geo_directory,
			graph_directory=graph_directory,
			graph_model_name=graph_model_name,
			gcp=gcp)

while True:
	# determine data category by bucket
	while True:
		data_type = input("Choose the category of data to add to your graph from 'residential', 'crime':   ")
		if data_type not in ['residential','crime']:
			print("Choose either 'residential' or 'crime':   ")
		else:
			if data_type == 'residential':
				data_directory_complete = data_directory + "res-bucket"
			elif data_type == 'crime':
				data_directory_complete = data_directory + "crim-bucket"
			break
	# determine data sources by file
	while True:
		if gcp:
			files_list = list(storage_client.get_bucket(data_directory_complete[5:]).list_blobs())
		else:
			files_list = os.listdir(data_directory_complete)

		result = [name if "standardized" in str(name) else None for name in files_list]
		file_name = input("Choose one of the following data sources, {}:   ".format(result))
		if file_name not in result:
			print("Choose one of the following data sources, {}:   ".format(result))
		else:
			break
	# determine geographic entity on which to aggregate
	while True:
		aggregate_by = input("Choose geographic entity to aggregate by from 'neighborhood' or 'tract' (i.e. census tract):   ")
		if aggregate_by not in ['neighborhood', 'tract']:
			print("Choose either 'neighborhood' or 'tract':   ")
		else:
			break
	# determine aggregate feature: aggregate_function pairs
	while True:
		df = pd.read_csv("{}/{}".format(data_directory_complete, file_name))
		cols = df.columns
		features = input("List the features that you would like to aggregate from the following. Add one space between each one, {}:   ".format(cols)).rstrip().split(" ")
		features = [feat.replace(" ","") for feat in features]
		overlap = list(set(features) - set(cols))
		if len(overlap) != 0:
			print("{} is/are not feature option(s):   ".format(overlap))
		else:
			feature_function_pairs = {}
			for feature in features:
				while True:
					aggregate_function = input("Choose an aggregating function for chosen feature, {}, from 'mean', 'count':   ".format(feature))
					if aggregate_function not in ['mean','count']:
						print("Choose either 'mean' or 'count:   ")
					else:
						break
				feature_function_pairs.update({feature: aggregate_function})
			break

	print("Creating structure of graph model...")
	gm.create_structure(aggregate_by=aggregate_by, 
				data_directory=data_directory_complete, 
				file_name=file_name, **feature_function_pairs)

	result = input("Graph structure created and saved as {}.pkl. Would you like to add to it by\
	 choosing another combination of data type, data file, geographic aggregator, and features?\
	 Answer 'yes' or 'no'   ".format(graph_model_name))
	if result not in ['yes','no']:
		print("Answer 'yes' or 'no':   ")
	elif result == 'no':
		finished = True
		break
	else:
		print("Let's add more to {}!".format(graph_model_name))


if finished:
	print("Adding graph structure to Neo4j!")
	gm.create_neo4j_queries()
	gm.query_neo4j()
	print("Done!")



















