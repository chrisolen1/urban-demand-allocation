import argparse
parser = argparse.ArgumentParser(description="geospatial_reformat_parser")
parser.add_argument("--home_directory", action="store", dest="home_directory", type=str, help="location of the home directory")
parser.add_argument("--geo_directory", action="store", dest="geo_directory", type=str, help="location of the geo shapefiles")
parser.add_argument("--data_directory", action="store", dest="data_directory", type=str, help="location of the relevant data directory")
parser.add_argument("--file_name", action="store", dest="file_name", type=str, help="name of the file we're updating")

parse_results = parser.parse_args()
home_directory = parse_results.home_directory
geo_directory = parse_results.geo_directory
res_directory = parse_results.res_directory
crime_directory = parse_results.crime_directory
bus_directory = parse_results.bus_directory
file_name = parse_results.file_name

import sys
sys.path.append(home_directory)

import pandas as pd
import numpy as np
import json
from tqdm import tqdm
import gcsfs

import utilities

"""
ensures that neighborhood and census tract naming conventions
are the same across all data sources
"""

# use polygon json files as naming standard

with open('{}/neighborhood_reformatted.json'.format(geo_directory),'r') as f:
    neighborhoods = json.load(f)
    
with open('{}/tract_reformatted.json'.format(geo_directory),'r') as f:
    tracts = json.load(f)

# standardize place names in residential data

df = pd.read_csv('{}/{}'.format(data_directory, file_name))

df = list(zip(df.longitude, df.latitude))

print("matching samples with neighborhoods")
n = [utilities.point_lookup(neighborhoods, positions[i]) for i in tqdm(range(len(df)))]
print("matching samples with tracts")
t = [utilities.point_lookup(tracts, positions[i]) for i in tqdm(range(len(df)))]

df['neighborhood'] = np.nan
df['tracts'] = np.nan

df['neighborhood'] = np.array(n)
df['tracts'] = np.array(t)

print("writing standardized csv")
properties.to_csv('{}/{}_standardized.csv'.format(data_directory, file_name))

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


