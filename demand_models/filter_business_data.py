import pandas as pd
import numpy as np

def business_filter(bus_frame, years, naics_codes):

	"""
	:bus_frame: business dataframe
	:years: list of integer years you would like to select out
	:naics_codes: list of string naics codes you would like to select out,
				will match only up to the length of the code provided
	Returns: business dataframe index by the latest year's abi with current and past year
				sales figures/employee info as features (versus as additional instances)
	"""

	assert(isinstance(years,list)), "\
		years argument must be of type list"
	
	assert(all(element for element in [isinstance(i,int) for i in years])), "\
		all years must be of type int"
	
	assert(isinstance(naics_codes,list)), "\
		naics_code argument must be of type list"
	
	assert(all(element for element in [isinstance(i,str) for i in naics_codes])), "\
		all naics_codes must be of type str"

	# filter for indicated years and naics_codes
	bus = naics_year_filter(bus_frame, years, naics_codes)
	# set meta data aside
	meta_data = bus[bus['year']==2017][['abi','year_established','latitude','longitude']]
	# drop meta data and other info used for filtering
	bus.drop(['primary_naics_code','company','business_status_code','company_holding_status',
		'year_established','latitude','longitude'], axis=1, inplace=True)
	# pull out the latest year 
	latest_year = max(years)

	# left join successive years of business data using latest year abi as left key
	for i in years:
		if i == latest_year:
			demand = bus[bus['year']==i]

		else:
			frame = bus[bus['year']==i]
			demand = pd.merge(how='left', left=demand, right=frame, left_on='abi', right_on='abi', suffixes = ("","_{}".format(str(i))))

	# merge meta data back in
	demand = pd.merge(how='left', left=demand, right=meta_data, left_on='abi', right_on='abi')  

	return demand      
		

def naics_year_filter(bus_frame, years, naics_codes):
	
	"""
	:bus_frame: business dataframe
	:years: list of integer years you would like to select out
	:naics_codes: list of string naics codes you would like to select out,
				will match only up to the length of the code provided
	Returns: dataframe of business data filtered for year and naics code
	"""
	
	bus_frame = bus_frame[bus_frame['primary_naics_code'].apply(parse_naics, args=[naics_codes])]
	
	return bus_frame[bus_frame['year'].isin(years)]
	
	
def parse_naics(df_value, naics):
	
	"""
	filter provided dataframe for naics codes. 
	mean to be used in df.apply() 
	"""

	results = []
	for i in naics:
		
		naics_length = len(i)
		truncated_naics = df_value[:naics_length]
		if truncated_naics == i:
			results.append(True)
		else:
			results.append(False)
			
	return any(results)
	
	