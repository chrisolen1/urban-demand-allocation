import argparse
parser = argparse.ArgumentParser(description="geospatial_reformat_parser")
parser.add_argument("--home_directory", action="store", dest="home_directory", type=str, help="location of the home directory")
parser.add_argument("--geo_directory", action="store", dest="geo_directory", type=str, help="location of the geo shapefiles")
parser.add_argument("--data_directory", action="store", dest="data_directory", type=str, help="location of the relevant data directory")
parser.add_argument("--file_name", action="store", dest="file_name", type=str, help="name of the file we're updating")
parser.add_argument("--geo_type", action="store", dest="geo_type", type=str, help="type of the geo shapefile")
parser.add_argument('--gcp', action='store_true', dest='gcp', help='affects whether to configure to running on the cloud')

parse_results = parser.parse_args()
home_directory = parse_results.home_directory
geo_directory = parse_results.geo_directory
file_name = parse_results.file_name
geo_type = parse_results.geo_type
data_directory = parse_results.data_directory
gcp = parse_results.gcp

import sys
sys.path.append(home_directory)

import pandas as pd
import numpy as np
import json
from p_tqdm import p_map
import multiprocessing as mp
import tqdm 
from functools import partial
if gcp:
    import gcsfs
    from google.cloud import storage
    storage_client = storage.Client()

import json
from shapely.geometry import shape, Point, Polygon
import re
from operator import itemgetter
#import utilities
def point_lookup(polygon_dict, point):

	"""
	assign place label corresponding to the provided
	(long, lat) or (lat, long) coordinate based on which 
	polygon from polygon_dict it fits in
	Note: the order of the coordinate tuple (lat,long) or (long,lat)
	depends the orderings present in the polygon_dict
	:polygon_dict: dict of locality coordinate lists, where each coordinate represents a 
					vertex of the locality shape.
	:point: tuple of coordinates corresponding to the point of interest
	Returns: Name of the locality to which the provided point corresponds, 
			name will be one of the polygon_dict keys, else returns "None"
	"""
	assert(isinstance(polygon_dict, dict)), "\
			polygon_dict argument must be of type dict"

	assert(isinstance(point, tuple)), "\
			point argument must be of type tuple"
	# convert to point object
	point = Point(point) 
	
	# iterate over locality keys
	for i in range(len(list(polygon_dict.keys()))):
		result = Polygon(polygon_dict[list(polygon_dict.keys())[i]][0]).contains(point) # zero index is there because of zoning type in zoning data
		
		# return the relevant key if we have a result
		if result == True:
			return list(polygon_dict.keys())[i]
			break

	return "None" 

def f_mp(positions):
    chunks = [positions[i::4] for i in range(4)]
    chunksize =len(chunks[0])
    print("chunksize", chunksize)
    pool = mp.Pool(processes=4)
 
    result = list(tqdm.tqdm(pool.imap(lookup, positions, 1000), total=len(positions)))

    return result

"""
ensures that neighborhood and census tract naming conventions
are the same across all data sources
"""

# use polygon json files as naming standard
if gcp:
    bucket = storage_client.get_bucket(geo_directory[5:])
    blob = bucket.blob('chicago_{}_reformatted.json'.format(geo_type))
    geo = json.loads(blob.download_as_string(client=None))

else:	
    with open('{}/chicago_{}_reformatted.json'.format(geo_directory, geo_type),'r') as f:
        geo = json.load(f)
    

df = pd.read_csv('{}/{}'.format(data_directory, file_name))

positions = list(zip(df.longitude, df.latitude))

print("matching samples with {}".format(geo_type))
#n = p_map(partial(utilities.point_lookup, geo), positions, num_cpus=4)
#lookup = partial(utilities.point_lookup, geo)
#if __name__ == '__main__':
#    n = imap_unordered_bar(lookup, positions)

global lookup
lookup = partial(point_lookup, geo)
#if __name__ == '__main__':
#    with mp.Pool(4) as p:
#       list(tqdm.tqdm(p.imap(lookup, positions), total=len(positions)))

n = f_mp(positions)

#[utilities.point_lookup(geo, positions[i]) for i in tqdm.tqdm(range(len(positions)))]

df['{}'.format(geo_type)] = np.nan

df['{}'.format(geo_type)] = np.array(n)

print("writing standardized csv")
df.to_csv('{}/{}_standardized.csv'.format(data_directory, file_name))

"""
# standardize place names in residential data

properties = pd.read_csv('{}/residential.csv'.format(data_directory))
properties.drop(['neighborhood'], axis=1, inplace=True)

positions = list(zip(properties.longitude, properties.latitude))

print("matching properties with neighborhoods")
n = [utilities.point_lookup(neighborhoods, positions[i]) for i in tqdm(range(len(properties)))]
print("matching properties with tracts")
t = [utilities.point_lookup(tracts, positions[i]) for i in tqdm(range(len(properties)))]

properties['neighborhood'] = np.nan
properties['tracts'] = np.nan

properties['neighborhood'] = np.array(n)
properties['tracts'] = np.array(t)

print("writing residential_standardized.csv")
properties.to_csv('{}/residential_standardized.csv'.format(data_directory))

# standardize place names in crime data 

crime = pd.read_csv('{}/crime.csv'.format(data_directory))

positions = list(zip(crime.longitude, crime.latitude))

print("matching crimes with neighborhoods")
n = [utilities.point_lookup(neighborhoods, positions[i]) for i in tqdm(range(len(crime)))]
print("matching crimes with tracts")
t = [utilities.point_lookup(tracts, positions[i]) for i in tqdm(range(len(crime)))]

crime['neighborhood'] = np.nan
crime['tracts'] = np.nan

crime['neighborhood'] = np.array(n)
crime['tracts'] = np.array(t)

print("writing crime_standardized.csv")
crime.to_csv('{}/crime_standardized.csv'.format(data_directory))
"""

