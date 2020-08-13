import sys
sys.path.append('/Users/chrisolen/Documents/uchicago_courses/optimization/project/urban-demand-allocation')

import pandas as pd
import numpy as np
import json
from tqdm import tqdm

import utilities

"""
ensures that neighborhood and census tract naming conventions
are the same across all data sources
"""

import argparse
parser = argparse.ArgumentParser(description="geospatial_reformat_parser")
parser.add_argument("--geo_data_directory", action="store", dest="geo_data_directory", type=str, help="relative location of the geo shapefiles")
parser.add_argument("--data_directory", action="store", dest="data_directory", type=str, help="relative location of the data directory")
parse_results = parser.parse_args()
geo_data_directory = parse_results.geo_data_directory
data_directory = parse_results.data_directory

# use polygon json files as naming standard

with open('{}/neighborhood_reformatted.json'.format(geo_data_directory),'r') as f:
    neighborhoods = json.load(f)
    
with open('{}/tract_reformatted.json'.format(geo_data_directory),'r') as f:
    tracts = json.load(f)

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


