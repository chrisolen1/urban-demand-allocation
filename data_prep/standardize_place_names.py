import pandas as pd
import numpy as np
import json
import utilities

"""
ensures that neighborhood and census tract naming conventions
are the same across all data sources
"""

# use polygon json files as standard

with open('../dicts/neighborhood_polys.json','r') as f:
    neighborhoods = json.load(f)
    
with open('../dicts/tract_polys.json','r') as f:
    tracts = json.load(f)

# residential 

properties = pd.read_csv('../../data/residential.csv')
properties.drop(['neighborhood'], axis=1, inplace=True)

positions = list(zip(properties.longitude, properties.latitude))

n = [utilities.point_lookup(neighborhoods, positions[i]) for i in range(len(properties))]
t = [utilities.point_lookup(tracts, positions[i]) for i in range(len(properties))]

properties['neighborhood'] = np.nan
properties['tracts'] = np.nan

properties['neighborhood'] = np.array(n)
properties['tracts'] = np.array(t)

properties.to_csv('../../data/residential_standardized.csv')

# crime

crime = pd.read_csv('../../data/crime.csv')

positions = list(zip(crime.longitude, crime.latitude))

n = [utilities.point_lookup(neighborhoods, positions[i]) for i in range(len(crime))]
t = [utilities.point_lookup(tracts, positions[i]) for i in range(len(crime))]

crime['neighborhood'] = np.nan
crime['tracts'] = np.nan

crime['neighborhood'] = np.array(n)
crime['tracts'] = np.array(t)

crime.to_csv('../../data/crime_standardized.csv')


