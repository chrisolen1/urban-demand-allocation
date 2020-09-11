import pandas as pd
import numpy as np

import io
import numpy as np
import json
import multiprocessing as mp
from tqdm import tqdm 
from functools import partial
import os

import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
import gcsfs

"""""""""""""""""""""""""""""""""""""""""""""""""""
functions for filtering and tagging raw datasets
"""""""""""""""""""""""""""""""""""""""""""""""""""


def geo_tag(home_directory, file_name, data_directory, geo_directory, geo_entities, city, gcp):

	"""
	Tag the provided file with the categories of the provided geographic entity.
	:home_directory: str, path to top level of the repository 
	:file_name: str, path to the name of the dataset we want to tag 
	:data_directory: str, path to the relevant data directory/storage bucket for the dataset indicated by file_name
	:geo_entities: list of strings, name of geographic entities to geo-tag by (e.g. "neighborhoods","census tracts")
	:gcp: boolean, whether processing is done locally or on gcp
	Return: geo-tagged data_frame, written to csv
	"""
	assert(isinstance(home_directory,str)),"\
		argument home_directory must be of type str"

	assert(isinstance(file_name,str)),"\
		argument file_name must be of type str"		

	assert(isinstance(data_directory,str)),"\
		argument data_directory must be of type str"

	assert(isinstance(geo_directory,str)),"\
		argument geo_directory must be of type str"

	assert(isinstance(geo_entities,list)), "\
		argument geo_entities must be of type list"
	
	assert(all(element for element in [isinstance(i,str) for i in geo_entities])), "\
		each geo_entity in geo_entities list must be of type str"

	assert(isinstance(city,str)),"\
		argument city must be of type str"

	import sys
	sys.path.append(home_directory)
	import utilities
	
	# read in dataset
	df = pd.read_csv('{}/{}'.format(data_directory, file_name))
	for geo_entity in geo_entities:
		
		# read in geographic shape file
		if gcp:
			import gcsfs
			from google.cloud import storage
			storage_client = storage.Client()
			bucket = storage_client.get_bucket(geo_directory[5:])
			blob = bucket.blob('{}_{}_reformatted.json'.format(city, geo_entity))
			geo = json.loads(blob.download_as_string(client=None))

		else:	
			with open('{}/{}_{}_reformatted.json'.format(geo_directory, city, geo_entity),'r') as f:
				geo = json.load(f)

		# pull out lat long coordiantes
		positions = list(zip(df.longitude, df.latitude))

		print("matching samples with {}".format(geo_entity))   

		# run through utilities.point_lookup func w/ multiprocessing
		lookup = partial(utilities.point_lookup, geo)
		pool = mp.Pool(processes=os.cpu_count())
		chunksize = 1000
		n = list(tqdm(pool.imap(lookup, positions, chunksize), total=len(positions)))

		# add result back to dataset
		df['{}'.format(geo_entity)] = np.nan
		df['{}'.format(geo_entity)] = np.array(n)

	# write to csv
	print("writing geo-tagged csv")
	df.to_csv('{}/{}_geotagged.csv'.format(data_directory, file_name.replace(".csv","")), index=False)  	


class spark_filter(object):

	"""
	Class of functions for filtering down large raw datasets in gcp
	via Spark.
	"""

	def __init__(self, n_spark_workers):

		"""
		Specify number of workers for Spark config
		and initialize gcp storage client.
		n_spark_workers: int, number of workers in spark cluster 
		Returns: n_spark_workers and storage_client as class variables s
		"""

		try:
			int(n_spark_workers)
		except:
			print("argument n_spark_workers must be integer")

		self.n_spark_workers = n_spark_workers
		import gcsfs
		from google.cloud import storage
		self.storage_client = storage.Client()

	def init_session(self):

		"""
		Configure and initialize spark context and spark session.
		Returns: SparkSession, SparkContext objects as class variables 
		"""

		config = pyspark.SparkConf().setAll([("spark.dynamicAllocation.enabled","True"),
									("spark.executor.cores",str(self.n_spark_workers))])
		self.sc = SparkContext(conf=config)
		self.ss = SparkSession(self.sc)
		

	def stop_session(self):

		"""
		End spark session.
		"""

		self.sc.stop()

	def apply_filter(self, data_type, year, city, state=None):

		"""
		Filter raw object storage file for city and year; write back to storage bucket.
		Filtering is a bit different depending on the raw data source. 
		:data_type: category of data to filter, usually chosen from a list of strings
		:year: int, year you want to select for 
		:city: str, city you want to select for
		:state: str, state you want to select for; only an option for residential dataset
		Returns: writes filtered spark dataframe back to storage bucket 
		"""

		assert(isinstance(data_type,str)),"\
		data_type must be of type str"
		
		assert(isinstance(year,int)),"\
		year must be of type int"

		assert(isinstance(city,str)),"\
		city must be of type str"

		if state != None:
			assert(isinstance(state,str)),"\
				state must be of type str"

		if data_type == "business":
			
			# connect to relevant gcs bucket
			bucket = self.storage_client.get_bucket('biz-bucket')
			print("reading in spark df")
			# read in as spark df
			bus = self.ss.read.csv("gs://biz-bucket/raw_business.csv", inferSchema=True, header=True, sep = ',')
			# rename year column; specific to infogroup business dataset 
			bus = bus.withColumnRenamed('archive_version_year','year')
			# specify which columns to keep; this may need to be adjusted in the future
			keep_list = ['year', 'abi',	'company', 'city', 'zipcode', 'primary_naics_code', 'business_status_code',	
						'company_holding_status', 'year_established', 'employee_size_location', 'sales_volume_location',
						'latitude', 'longitude', 'neighborhood']
			bus = bus.select([column for column in bus.columns if column in keep_list])
			# apply filtering
			bus = bus.filter((col("city")==city.upper()) | (col("city")==city.lower())).filter(col("archive_version_year")==year)
			# transfer to pandas
			bus = bus.toPandas()
			# recoding cat variables; specific to infogroup dataset 
			bus['business_status_code'].replace(to_replace={1:'headquarters', 2:'branch', 3:'subsidiary', 9:'single_location'}, inplace=True)
			bus['company_holding_status'].replace(to_replace={np.nan:"private",1.0:"public"}, inplace=True)
			# remove naics code nulls and convert to string
			bus = bus[~bus['primary_naics_code'].isna()]
			naics_converted = bus['primary_naics_code'].apply(lambda x: str(int(x)))
			bus.loc[:,'primary_naics_code'] = naics_converted
			print("uploading filtered df to storage")
			# upload to cloud storage    
			bucket.blob('business_{}_{}.csv'.format(city, year)).upload_from_string(bus.to_csv(index=False), 'text/csv')

		elif data_type == 'residential':

			# connect to relevant gcs bucket 
			bucket = self.storage_client.get_bucket('res-bucket')
			print("reading in spark df")
			# read in as spark df 
			res = self.ss.read.csv("gs://res-bucket/raw_res_{}.txt".format(year), inferSchema=True, header=False, sep = '\t')
			# columns must be renamed; specific to residential infogroup dataset 
			res = res.withColumnRenamed('_c0','household_id').withColumnRenamed('_c1','location_type')\
				.withColumnRenamed('_c2','length_of_residence').withColumnRenamed('_c3','children_count')\
				.withColumnRenamed('_c4','hh_wealth').withColumnRenamed('_c5','hh_income')\
				.withColumnRenamed('_c6','owner_renter_status').withColumnRenamed('_c7','property_value')\
				.withColumnRenamed('_c8','marital_status').withColumnRenamed('_c9','street_number')\
				.withColumnRenamed('_c10','street_pre_direction').withColumnRenamed('_c11','street_name')\
				.withColumnRenamed('_c12','street_post_direction').withColumnRenamed('_c13','street_type')\
				.withColumnRenamed('_c14','unit_type').withColumnRenamed('_c15','unit_number')\
				.withColumnRenamed('_c16','city').withColumnRenamed('_c17','state')\
				.withColumnRenamed('_18','vacant').withColumnRenamed('_c19','latitude')\
				.withColumnRenamed('_c20','longitude').withColumnRenamed('_c21','census_tract')\
				.withColumnRenamed('_c22','ethnicity_code').withColumnRenamed('_c23','year')
			# specify which columns to keep; these may need to be adjusted in the future 
			keep_list = ['household_id', 'location_type', 'length_of_residence', 'children_count', 'hh_wealth', 'hh_income', 
						'owner_renter_status', 'property_value', 'street_pre_direction', 'street_name', 'unit_type', 'city', 
						'state', 'vacant', 'latitude', 'longitude', 'ethnicity_code']
			res = res.select([column for column in res.columns if column in keep_list])
			# apply filtering
			res = res.filter((col("city")==city.upper()) | (col("city")==city.lower())).filter((col("state")==state.upper()) | (col("state")==state.lower()))
			# transfer to pandas
			res = res.toPandas()
			print("uploading filtered df to storage")
			# upload to cloud storage   
			bucket.blob('residential_{}_{}.csv'.format(city, year)).upload_from_string(res.to_csv(index=False), 'text/csv')

		elif data_type == 'crime':

			# connect to relevant gcs bucket 
			bucket = self.storage_client.get_bucket('crim-bucket')
			print("reading in spark df")
			# read in as spark df 
			crime = self.ss.read.csv("gs://crim-bucket/raw_crime_{}.csv".format(city), inferSchema=True, header=True, sep = ',')
			crime = crime.withColumnRenamed('primary_type','crime_type')
			# specify which columns to keep; these may need to be adjusted in the future 
			keep_list = ['id','crime_type','description','arrest','domestic','year','latitude','longitude']
			crime = crime.select([column for column in crime.columns if column in keep_list])
			# apply filtering
			crime = crime.filter(col("year")==year)
			# transfer to pandas
			crime = crime.toPandas()
			print("uploading filtered df to storage")
			# upload to cloud storage    
			bucket.blob('crime_{}_{}.csv'.format(city, year)).upload_from_string(crime.to_csv(index=False), 'text/csv')







