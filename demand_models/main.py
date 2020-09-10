import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--home_directory", action="store", dest="home_directory", type=str, help="location of the home directory")
parser.add_argument("--geo_directory", action="store", dest="geo_directory", type=str, help="location of the geo shapefiles")
parser.add_argument("--data_directory", action="store", dest="data_directory", type=str, help="location of the relevant data directory")
parser.add_argument("--graph_directory", action="store", dest="graph_directory", type=str, help="location of the graph models")
parser.add_argument("--opt_directory", action="store", dest="opt_directory", type=str, help="location of the optimization variables")
parser.add_argument('--gcp', action='store_true', dest='gcp', help='affects whether to configure to running on the cloud')

parse_results = parser.parse_args()
home_directory = parse_results.home_directory
geo_directory = parse_results.geo_directory
data_directory = parse_results.data_directory
graph_directory = parse_results.graph_directory
opt_directory = parse_results.opt_directory
gcp = parse_results.gcp

import sys
sys.path.append(home_directory)
import os
import subprocess
import re
import pandas as pd
import numpy as np
import json

if gcp:
	import gcsfs
	from google.cloud import storage
	storage_client = storage.Client()

import utilities
from demand_models.build_demand_model_utils import business_filter, connect_to_neo4j, graph_to_demand_model
from data_prep.filter_utils import standardize_place_names

# load in raw business data 
#df_types = pd.read_csv('../../data/dtypes.csv')['dtypes']
#dtype=df_types.to_dict()
city = input("Welcome to the demand model module! What city would you like to examine?:   ").lower()
year = int(input("Please list the year of business data you are interested in. Year must be between\
	2010 and 2018. You can select more years later:   "))
while True:
	if year < 2010 and year > 2018:
		year = ("Year must be between 2010 and 2018. Please reselect:   ")
	else:
		break
print("loading in business data...")
bus = pd.read_csv("{}biz-bucket/business_{}_{}_standardized.csv".format(data_directory, city, year))
bus['primary_naics_code'] = bus['primary_naics_code'].apply(lambda x: str(x))
naicses = []
naics = input("Please choose a naics code between four and eight digits:    ")
while True:
	try:
		int(naics)
		if len(naics) < 4 or len(naics) > 8:
			naics = input("Please choose a naics code between four and eight digits:    ")
		else:
			naicses.append(naics)
			break
	except ValueError: 
		naics = input("Naics codes must be integer values:    ")
while True:
	result = input("Would you like to include another naics code in your search? Answer 'yes' or 'no':   ")
	if result not in ['yes','no']:
		print("Answer 'yes' or 'no':   ")
	elif result == 'no':
		break
	else:
		naics = input("Please choose a naics code between four and eight digits:    ")
		while True:
			try:
				int(naics)
				if len(naics) < 4 or len(naics) > 8:
					naics = input("Please choose a naics code between four and eight digits:    ")
				else:
					naicses.append(naics)
					break
			except ValueError: 
				naics = input("Naics codes must be integer values:    ")
print("pulling relevant demand data...")				
demand = business_filter(bus, naicses)
print("writing to storage...")
if gcp:
	naics_str = "".join(i + "_" for i in naicses)
	opt_bucket = storage_client.get_bucket(opt_directory)
	bucket.blob('{}_{}_{}/{}.csv'.format(city, year, naics_str, naics_str)).upload_from_string(demand.to_csv(index=False), 'text/csv')
else:
	naics_str = "".join(i + "_" for i in naicses)
	os.system("mkdir {}/{}_{}_{}".format(opt_directory, city, year, naics_str))
	demand.to_csv("{}/{}_{}_{}/{}.csv".format(opt_directory, city, year, naics_str, naics_str), index=False)
full_path = "{}/{}_{}_{}".format(opt_directory, city, year, naics_str)
answer = int(input("Let's determine the predictors for our demand model. (1) Do you have an existing\
 graph model you're working off of or (2) would you like create a new graph?:   "))
while True:
	if answer != 1 and answer != 2:
		answer = input("Please answer the above with '1' or '2':   ")
	else:
		break
if answer == 2:
	from graph_utils import graph_model
	graph_model_name = input("Provide the title of your graph: ")
	gm = graph_model(home_directory=home_directory,
			geo_directory=geo_directory,
			graph_directory=graph_directory,
			graph_model_name=graph_model_name,
			gcp=gcp)
	demand_model_features = []
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
		if gcp:
			files_list = list(storage_client.get_bucket(data_directory_complete[5:]).list_blobs())
			result = [str(name).split(',')[1].replace(" ","") if "standardized" in str(name) else None for name in files_list]
		else:
			files_list = os.listdir(data_directory_complete)
			result = [name if "standardized" in str(name) else None for name in files_list]

		results = []
		for name in result:
			if name != None:
				results.append(name) 

		results = {a:b for (a,b) in list(enumerate(results))}
		print(list(results.keys()))
		while True:
			file_number = int(input("Choose the number corresponding to one of the following data sources, {}:   ".format(str(results))))
			if file_number not in list(results.keys()):
				print("Choose the number corresponding to one of the following data sources, {}:   ".format(str(results)))
			else:
				file_name = results[file_number]
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
			print("loading {}/{} ...".format(data_directory_complete, file_name))
			df = pd.read_csv("{}/{}".format(data_directory_complete, file_name))
			cols = df.columns
			features = input("List the features that you would like to aggregate from the following. Add one space between each one, {}:   ".format(cols)).rstrip().split(" ")
			features = [feat.replace(" ","") for feat in features]
			demand_model_features += features
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
	geo_types = aggregate_by
	print("Done with graph model!")

elif answer == 1:
	print("retrieving node categories from existing graph")
	geo_types = str(subprocess.check_output("kubectl exec -it neo4j-ce-1-0 -- cypher-shell \
		-u 'neo4j' -p 'asdf' -d 'neo4j' --format plain 'match (n) return distinct labels(n)'", shell=True))
	geo_types = re.findall("(\[[\W\w]+\])",geo_types)[0].replace("]","").replace("[","").replace('"',"").split(",")
	print("retrieving attribute features from existing graph")
	for geo_type in geo_types:
		features = str(subprocess.check_output("kubectl exec -it neo4j-ce-1-0 -- cypher-shell \
			-u 'neo4j' -p 'asdf' -d 'neo4j' --format plain 'match (a:{}) return keys(a)'".format(geo_type), shell=True))
		features = re.findall("(\[[\W\w]+\])",features)[0].replace("]","").replace("[","").replace('"',"").split("\\r\\n")[0].split(",")
		features = [i.replace(" ","") for i in features]
		
print("features",features)
print("addng geo entities to demand data...")
file_name = naics_str + ".csv"
standardize_place_names(home_directory, file_name, full_path, geo_directory, geo_types, city, gcp)
# load geo tagged demand data 
demand = pd.read_csv("{}/{}".format(full_path, file_name))

#### some line of question re edge relations for demand model 
geo_entity = geo_types[0]
demand = graph_to_demand_model(demand, geo_directory, geo_entity, city)


"""
demand = graph_to_demand_model(demand, "primary_type", neighborhoods, "neighborhood")
demand = graph_to_demand_model(demand, "zestimate", neighborhoods, "neighborhood", edge_relation="NEXT_TO")
demand = graph_to_demand_model(demand, "primary_type", neighborhoods, "neighborhood", edge_relation="NEXT_TO")
"""

# remove any null values
demand.dropna(inplace=True)
demand.to_csv("{}/{}_{}_{}/demand_model.csv".format(opt_directory, city, year, naics_str), index=False)

