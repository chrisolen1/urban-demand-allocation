import pandas as pd
import numpy as np

import io
import numpy as np
import json
import multiprocessing as mp
from tqdm import tqdm 
from functools import partial

import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
import gcsfs
from google.cloud import storage

def standardize_place_names(home_directory, file_name, data_directory, geo_directory, geo_types, city, n_processes, gcp):

	import sys
	sys.path.append(home_directory)
	
	import utilities
        print(data_directory)
        print(file_name)
	
	df = pd.read_csv('{}/{}'.format(data_directory, file_name))

	for geo_entity in geo_types:
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

	
		positions = list(zip(df.longitude, df.latitude))

		print("matching samples with {}".format(geo_entity))   

		if n_processes > 1:
			lookup = partial(utilities.point_lookup, geo)
			pool = mp.Pool(processes=n_processes)
			chunksize = 1000
			n = list(tqdm(pool.imap(lookup, positions, chunksize), total=len(positions)))

		else:
			n = [utilities.point_lookup(geo, positions[i]) for i in tqdm(range(len(positions)))]

		df['{}'.format(geo_entity)] = np.nan

		df['{}'.format(geo_entity)] = np.array(n)

	print("writing standardized csv")
	df.to_csv('{}/{}_standardized.csv'.format(data_directory, file_name.replace(".csv","")))  	


class spark_filter(object):

	def __init__(self, n_spark_workers):



		self.n_spark_workers = n_spark_workers
		# connect to cloud storage
		self.storage_client = storage.Client()



	def init_session(self):

		"""
		Configure and initialize spark context and spark session
		:n_spark_workers: str integer
		Returns: SparkSession, SparkContext objects, respectively
		"""

		assert(isinstance(self.n_spark_workers,str)),"\
		n_spark_workers must be of type str"

		config = pyspark.SparkConf().setAll([("spark.dynamicAllocation.enabled","True"),
									("spark.executor.cores",self.n_spark_workers)])
		self.sc = SparkContext(conf=config)
		self.ss = SparkSession(self.sc)
		

	def stop_session(self):

		self.sc.stop()

	def apply_filter(self, data_type, year, city, state=None):

		"""
		Filter raw object storage file for city and year; write back to storage bucket
		:spark_session: live spark session object
		:year: int, year you want to select for 
		:city: str, city you want to select for
		:overwrite: bool, if True, will overwrite any existing file of the same year and city specifications
		Returns: writes filtered spark dataframe back to storage bucket 
		"""

		assert(isinstance(year,int)),"\
		year must be of type int"

		assert(isinstance(city,str)),"\
		city must be of type str"

		if state != None:
			assert(isinstance(state,str)),"\
				state must be of type str"
	
		if data_type == "business":
			
			bucket = self.storage_client.get_bucket('biz-bucket')
			print("reading in spark df")
			# read to spark df
			bus = self.ss.read.csv("gs://biz-bucket/raw_business.csv", inferSchema=True, header=True, sep = ',')
			# drop currently un-needed columns
			drop_list = ['ticker', 'address_line_1','location_employee_size_code',
			'location_sales_volume_code','sic_code','sic6_descriptions_sic','office_size_code',
			'parent_employee_size_code','parent_sales_volume_code','census_tract','cbsa_code',
			'parent_actual_employee_size','parent_actual_sales_volume']
			bus = bus.select([column for column in bus.columns if column not in drop_list])
			# apply filtering
			bus = bus.filter(col("city")==city.upper() | col("city")==city.lower()).filter(col("archive_version_year")==year)
			# transfer to pandas
			bus = bus.toPandas()
			# recoding cat variables
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

			bucket = self.storage_client.get_bucket('res-bucket')
			print("reading in spark df")
			# read to spark df 
			res = self.ss.read.csv("gs://res-bucket/raw_res_{}.txt".format(year), inferSchema=True, header=False, sep = '\t')
			# columns must be renamed
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
			# drop currently un-needed columns
			drop_list = ['street_number', 'street_post_direction', 'street_type','unit_number',
			'location_sales_volume_code','sic_code','sic6_descriptions_sic','office_size_code',
			'census_tract','marital_status']
			res = res.select([column for column in res.columns if column not in drop_list])
			# apply filtering
			res = res.filter((col("city")==city.upper()) | (col("city")==city.lower())).filter((col("state")==state.upper()) | (col("state")==state.lower()))
			# transfer to pandas
			res = res.toPandas()
			print("uploading filtered df to storage")
			# upload to cloud storage    
			bucket.blob('residential_{}_{}.csv'.format(city, year)).upload_from_string(res.to_csv(index=False), 'text/csv')
