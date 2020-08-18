import argparse
parser = argparse.ArgumentParser(description="geospatial_reformat_parser")
parser.add_argument("--home_directory", action="store", dest="home_directory", type=str, help="location of the home directory")
parser.add_argument("--geo_directory", action="store", dest="geo_directory", type=str, help="location of the geo shapefiles")
parser.add_argument("--data_directory", action="store", dest="data_directory", type=str, help="location of the relevant data directory")
parser.add_argument("--file_name", action="store", dest="file_name", type=str, help="name of the file we're updating")
parser.add_argument("--geo_type", action="store", dest="geo_type", type=str, help="type of the geo shapefile")
parser.add_argument("--n_cores", action="store", dest="n_cores", type=str, help="number of cores to be used for processing")
parser.add_argument('--gcp', action='store_true', dest='gcp', help='affects whether to configure to running on the cloud')

parse_results = parser.parse_args()
home_directory = parse_results.home_directory
geo_directory = parse_results.geo_directory
file_name = parse_results.file_name
geo_type = parse_results.geo_type
data_directory = parse_results.data_directory
n_cores = parse_results.n_cores
gcp = parse_results.gcp

import sys
sys.path.append(home_directory)

import pandas as pd
import numpy as np
import json
import multiprocessing as mp
import tqdm 
from functools import partial
if gcp:
    import gcsfs
    from google.cloud import storage
    storage_client = storage.Client()

import utilities

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

if n_cores > 1:
	lookup = partial(utilities.point_lookup, geo)
	pool = mp.Pool(processes=4)
	chunksize = 1000
	n = list(tqdm.tqdm(pool.imap(lookup, positions, chunksize), total=len(positions)))

else:
	n = [utilities.point_lookup(geo, positions[i]) for i in tqdm.tqdm(range(len(positions)))]

df['{}'.format(geo_type)] = np.nan

df['{}'.format(geo_type)] = np.array(n)

print("writing standardized csv")
df.to_csv('{}/{}_standardized.csv'.format(data_directory, file_name.replace(".csv","")))





