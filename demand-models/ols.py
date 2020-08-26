import sys
sys.path.append('/Users/chrisolen/Documents/uchicago_courses/optimization/project/urban-demand-allocation')

import pandas as pd
import numpy as np
import csv

from sklearn.linear_model import LinearRegression

import utilities
from demand_models.build_demand_model_utils import business_filter, connect_to_neo4j, graph_to_demand_model

# read in cleaned up demand model data 
demand = pd.read_csv("../../data/demand_model.csv")

# split into features and response variable
demand_features = demand[[i for i in demand.columns if "sales" not in i]]
demand_features.drop(['latitude','longitude','year'], axis=1, inplace=True)
y = demand['sales_volume_location']
X = np.array(demand_features)

# fit ols regression
lr = LinearRegression()
demand_model = lr.fit(X,y)

# extract and save betas 
_betas = list(demand_model.coef_)
file = open("../../data/betas.csv", 'w+', newline='')
with file:
    write = csv.writer(file)
    write.writerows([_betas])
