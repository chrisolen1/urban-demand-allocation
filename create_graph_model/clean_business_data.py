import pandas as pd
import numpy as np

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
bus = pd.read_csv("../../data/chi_bus.csv", sep='\t', names=cols, low_memory=False)

# retain only model features
model_features = ['abi','primary_naics_code','company','year','business_status_code','company_holding_status','year_established',
           'employee_size_location','sales_volume_location','latitude','longitude']

bus = bus[model_features]

# understanding business status code
bus['business_status_code'].replace(to_replace={1:'headquarters', 2:'branch', 3:'subsidiary', 9:'single_location'}, inplace=True)

# understanding business status holding
bus['company_holding_status'].replace(to_replace={np.nan:"private",1.0:"public"}, inplace=True)

# remove naics code nulls and convert to string
bus = bus[~bus['primary_naics_code'].isna()]
naics_converted = bus['primary_naics_code'].apply(lambda x: str(int(x)))
bus.loc[:,'primary_naics_code'] = naics_converted

bus.to_csv("../../data/chi_bus_cleaned.csv", index=False)
bus.dtypes.to_frame('dtypes').to_csv('../../data/dtypes.csv')
print("wrote cleaned business data to ../../data/chi_bus_cleaned.csv")


