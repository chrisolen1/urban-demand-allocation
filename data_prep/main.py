import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--home_directory", action="store", dest="home_directory", type=str, help="location of the home directory")
parser.add_argument("--geo_directory", action="store", dest="geo_directory", type=str, help="location of the geo shapefiles")
parser.add_argument("--data_directory", action="store", dest="data_directory", type=str, help="location of the relevant data directory")
parser.add_argument("--n_spark_workers", action="store", dest="n_spark_workers", type=str, help="number of workers to be used for spark session")
parser.add_argument('--gcp', action='store_true', dest='gcp', help='affects whether to configure to running on the cloud')

parse_results = parser.parse_args()
home_directory = parse_results.home_directory
geo_directory = parse_results.geo_directory
data_directory = parse_results.data_directory
n_spark_workers = parse_results.n_spark_workers
gcp = parse_results.gcp

import os

# connect to gcs if gcp flag provided
if gcp:
	import gcsfs
	from google.cloud import storage
	storage_client = storage.Client()
	
import filter_utils	

city = input("Welcome to the data store! What city would you like to examine?:   ").lower()
answer = input("Would you like to add a state name to clarify the city? Answer 'yes' or 'no':   ")
while True:	
	if answer not in ['yes','no']:
		answer = input("Answer 'yes' or 'no':   ")
	elif answer == 'yes':
		state = input("Indicate the state:   ")
		break
	else:
		state = None
		break
data_type = input("Please select the type of data pertaining to this city that you are\
	interested in from 'residential', 'business', or 'crime':   ").lower()
while True:
	if data_type not in ['residential','crime','business']:
		data_type = input("Please choose either 'residential', 'crime', or 'business'")
	else:
		if data_type == 'residential':
				data_directory_complete = data_directory + "res-bucket"
				
		elif data_type == 'crime':
				data_directory_complete = data_directory + "crim-bucket"
				
		elif data_type == 'business':
				data_directory_complete = data_directory + "biz-bucket"						
		break

year = int(input("Please list the year you are interested in. Year must be between\
	2010 and 2018. You can select more years later:   "))
while True:
	if year < 2010 and year > 2018:
		year = int(input("Year must be between 2010 and 2018. Please reselect:   "))
	else:
		break
if gcp:
	files_list = list(storage_client.get_bucket(data_directory_complete[5:]).list_blobs())		
else:
	files_list = os.listdir(data_directory_complete)
result = [1 if "{}_{}_{}".format(data_type,city,year) in str(name).lower() else 0 for name in files_list]
while True:
	if sum(result) < 1:
		print("Preparing data...")
		sf = filter_utils.spark_filter(n_spark_workers)
		sf.init_session()
		sf.apply_filter(data_type, year, city, state=state)
		print("Ending Spark session...")
		sf.stop_session()
		break
	else:
		break
print("This data is already ready to go! Let's see if we need to standardize the place names...")
result = [1 if "{}_{}_{}_standardized".format(data_type,city,year) in str(name).lower() else 0 for name in files_list]
while True:
	if sum(result) < 1:
		geo_entities = input("Let's standardize: Which of the following geographic entities would you like\
			to add to the data set?. Put one space between each:   'neighborhood', 'tract'    ")
		geo_entities = geo_entities.rstrip().split(" ")
		while True:
			result = any([False if entity not in ['neighborhood','tract'] else True for entity in geo_entities])
			if not result:
				geo_entities = input("Choose from the following. Put one space between each:    'neighborhood', 'tract'    ")
				geo_entities = geo_entities.rstrip().split(" ")
			else:
				break
		for entity in geo_entities:
			
		print("Standardizing place names...")
		file_name = "{}_{}_{}.csv".format(data_type, city, year)
		filter_utils.geo_tag(home_directory, file_name, data_directory_complete, geo_directory, geo_entities, city, gcp)
		print("Done!")
		break
	else:
		print("This data is already standardized!")
		break







	





