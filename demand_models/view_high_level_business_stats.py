import pandas as pd
import numpy as np

"""
print high-level information regarding the business 
within the 
"""


# specify column names 
cols=['year',
 'abi',
 'ticker',
 'company',
 'address_line_1',
 'city',
 'zipcode',
 'location_employee_size_code',
 'location_sales_volume_code',
 'primary_naics_code',
 'sic_code',
 'sic6_descriptions_sic',
 'business_status_code',
 'office_size_code',
 'company_holding_status',
 'parent_employee_size_code',
 'parent_sales_volume_code',
 'census_tract',
 'cbsa_code',
 'year_established',
 'employee_size_location',
 'sales_volume_location',
 'parent_actual_employee_size',
 'parent_actual_sales_volume',
 'latitude',
 'longitude']

# load sales data from infogroup
bus = pd.read_csv("../../data/chi_bus.csv", sep='\t', names=cols)

# retain only model features

model_features = ['abi','primary_naics_code','company','year','business_status_code','company_holding_status',
           'census_tract','year_established',
           'employee_size_location','sales_volume_location',
           'latitude','longitude']

bus = bus.loc[:,model_features]

print("number of samples:", bus.shape[0])

# understanding business status code

bus['business_status_code'].replace(to_replace={1:'headquarters', 2:'branch', 3:'subsidiary', 9:'single_location'}, inplace=True)
print("\nbusiness_status_code: \n", bus['business_status_code'].value_counts())

# understanding business status holding

bus['company_holding_status'].replace(to_replace={np.nan:"private",1.0:"public"}, inplace=True)
print("\ncompany_holding_status: \n",bus['company_holding_status'].value_counts())

# understanding year distribution 

print("\ndistribution of sample across years: \n", bus['year'].value_counts())



