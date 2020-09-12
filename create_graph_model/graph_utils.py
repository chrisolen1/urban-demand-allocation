import pandas as pd
import numpy as np
import json

import os

import networkx as nx
from py2neo import Graph
import re
from itertools import combinations

import multiprocessing as mp
from tqdm import tqdm

from functools import partial

import utilities

"""""""""""""""""""""""""""""""""""""""""""""""""""
functions for creating neo4j graphs out of relational
data via python
"""""""""""""""""""""""""""""""""""""""""""""""""""


class graph_model(object):

	"""
	Class of functions for building graphs out of 
	Python's networkx module and translating them o
	to Neo4j format. 
	"""

	def __init__(self, home_directory,
			geo_directory,
			graph_directory,
			graph_model_name,
			gcp):

		"""
		:home_directory: str, top level of repository
		:geo_directory: str, location of geographic entity shape files 
		:graph_directory: str, location in which we'll store networkx pkl files and neo query txt files
		:graph_model_name: str, name of graph to be constructed
		:gcp: boolean, whether processing is to be done on gcp
		"""

		assert(isinstance(home_directory,str)),"\
			argument home_directory must be of type str"

		assert(isinstance(geo_directory,str)),"\
			argument geo_directory must be of type str"

		assert(isinstance(graph_directory,str)),"\
			argument graph_directory must be of type str"

		assert(isinstance(geo_model_name,str)),"\
			argument graph_model_name must be of type str"

		assert(isinstance(gcp,bool)),"\
			argument gcp must be of type boolean"	


		self.home_directory = home_directory
		self.geo_directory = geo_directory
		self.graph_directory = graph_directory
		self.graph_model_name = graph_model_name
		self.gcp = gcp

		# set path of top-level directory
		import sys
		sys.path.append(self.home_directory)

		# make geo and graph buckets class variables if gcp
		if self.gcp:
			import gcsfs
			from google.cloud import storage
			storage_client = storage.Client()
			self.geo_bucket = storage_client.get_bucket(geo_directory[5:])
			self.graph_bucket = storage_client.get_bucket(graph_directory[5:])


	def create_structure(self,aggregate_by,
						data_directory,
						file_name,
						**feature_function_pairs):

		"""
		Creates structure of graph from provided relational datasets 
		using python networkx.
		:aggregate_by: str, geographic entity by which to aggregate relational data. This
		will become the nodes of the graph and will be a column in the geo-tagged relational dataset.
		:data_directory: str, path to the relevant data directory/storage bucket 
		for the dataset indicated by file_name
		:file_name: str, path to the name of the geo-tagged dataset we want to use to populate the graph
		node attributes
		:**feature_function_pairs: kwargs, feature to be aggregated from relational dataset
		as the key and the aggregation function to be applied as its value
		Returns: Writes resulting graph structre as pkl to disk in graph bucket. 
		"""

		assert(isinstance(aggregate_by, str)),"\
			argument aggrebate_by must be of type str"

		assert(isinstance(data_directory, str)),"\
			argument data_directory must be of type str"

		assert(isinstance(file_name, str)),"\
			argument file_name must be of type str"										

		# load in geographic entity shape files
		if self.gcp:
			blob = self.geo_bucket.blob('chicago_{}_reformatted.json'.format(aggregate_by))
			geo_entities = json.loads(blob.download_as_string(client=None))
		else:
			with open('{}/chicago_{}_reformatted.json'.format(self.geo_directory, aggregate_by),'r') as f:
				geo_entities = json.load(f)

		# load the requested specified dataframe 
		df = pd.read_csv('{}/{}'.format(data_directory, file_name))
		# aggregate features from dataframe to populate graph node attributes 
		df_aggregated = self.aggregate_features(df, geo_entities, aggregate_by, **feature_function_pairs)

		# determine whether a serialized version of this graph_model already exists 
		exists = ""
		if self.gcp:
			graph_list = list(self.graph_bucket.list_blobs())
			result = [1 if self.graph_model_name+'.pkl' in str(name) else 0 for name in graph_list]
			if sum(result) >= 1:
				exists = True
				ix = result.index(1)
			else:
				exists = False

		else:
			graph_list = os.listdir(self.graph_directory)
			result = [1 if self.graph_model_name+'.pkl' in name else 0 for name in graph_list]
			if sum(result) >= 1:
				exists = True
				ix = result.index(1)
			else:
				exists = False

		# if there isn't already a serialized version of this graph_model
		# we create nodes named after 'aggregate_by' with attributes corresponding 
		# created by aggregating the dataframe features 
		if not exists:
	
			# first create nodes, return resulting graph structure
			G = self.create_pynx_nodes(df_aggregated, node_category=aggregate_by,
									attribute_columns=list(df_aggregated.columns))

			# then create edge relationships between nodes
			# current the only option is "NEXT_TO"
			G = self.add_edges_to_pynx(G, "NEXT_TO", utilities.intersection, ["polygon_name_1", "polygon_name_2"], \
									aggregate_by, bidirectional=True, polygon_dict_1=geo, \
									polygon_dict_2=geo)

		# if the serialized version of the graph_model already exists,
		# we load it into memory and using networkx to add nodes, attributes, edge relations
		# that dont already exist 
		if exists:

			print("adding to existing graph")
			if self.gcp:
		
				blob = self.graph_bucket.blob('{}.pkl'.format(self.graph_model_name))
				os.system("mkdir {}/create_graph_model/temp".format(self.home_directory))
				blob.download_to_filename("{}/create_graph_model/temp/temp.pkl".format(self.home_directory), client=None)
				G = nx.read_gpickle("{}/create_graph_model/temp/temp.pkl".format(self.home_directory))
				os.remove("{}/create_graph_model/temp/temp.pkl".format(self.home_directory))
				os.system("rmdir {}/create_graph_model/temp".format(self.home_directory))
	
			else:
		
				G = nx.read_gpickle("{}/{}.pkl".format(self.graph_directory, self.graph_model_name))

			# we note the 'existing_graph' kwarg is specified here 
			G = self.create_pynx_nodes(df_aggregated, node_category=aggregate_by, 
									attribute_columns=list(df_aggregated.columns), existing_graph=G)

			G = self.add_edges_to_pynx(G, "NEXT_TO", utilities.intersection, ["polygon_name_1", "polygon_name_2"], \
									aggregate_by, bidirectional=True, polygon_dict_1=geo, \
									polygon_dict_2=geo)

		"""
		RETURN TO THIS: SEPARATE SCRIPT FOR MULTIPLE GEO FILES
		# create unidirectional edges between census tract and neighborhood
		G = pynx_to_neo4j.add_edges_to_pynx(G, "CONTAINS", utilities.intersection, ["polygon_name_1", "polygon_name_2"], \
									"neighborhood", "tract", bidirectional=False, polygon_dict_1=neighborhoods, \
									polygon_dict_2=tracts)
		G = pynx_to_neo4j.add_edges_to_pynx(G, "IS_WITHIN", utilities.intersection, ["polygon_name_1", "polygon_name_2"], \
									"tract", "neighborhood", bidirectional=False, polygon_dict_1=tracts, \
									polygon_dict_2=neighborhoods)
		"""

		# save as pkl file
		if self.gcp:
			# write to disk first
			os.system("sudo mkdir {}/create_graph_model/temp".format(self.home_directory))
			nx.write_gpickle(G, "{}/create_graph_model/temp/{}.pkl".format(self.home_directory, self.graph_model_name))
			# then upload to bucket
			blob = self.graph_bucket.blob('{}.pkl'.format(self.graph_model_name))
			blob.upload_from_filename("{}/create_graph_model/temp/{}.pkl".format(self.home_directory, self.graph_model_name))
			os.remove("{}/create_graph_model/temp/{}.pkl".format(self.home_directory, self.graph_model_name))
			os.system("rmdir {}/create_graph_model/temp".format(self.home_directory))
	
		else:
			nx.write_gpickle(G, "{}/{}.pkl".format(self.graph_directory, self.graph_model_name))

	def create_neo4j_queries(self):

		"""
		Translate graph from pickled networkx format to a series of Neo4j queries in txt file.
		Returns: Writes resulting list of Neo4j queries to txt file. 
		"""

		# load into pickled networkx file
		if self.gcp:
			blob = self.graph_bucket.blob('{}.pkl'.format(self.graph_model_name))
			os.system("mkdir {}/create_graph_model/temp".format(self.home_directory))
			blob.download_to_filename("{}/create_graph_model/temp/temp.pkl".format(self.home_directory), client=None)
			G = nx.read_gpickle("{}/create_graph_model/temp/temp.pkl".format(self.home_directory))
			os.remove("{}/create_graph_model/temp/temp.pkl".format(self.home_directory))
			os.system("rmdir {}/create-graph_model/temp".format(self.home_directory))

		else:
			G = nx.read_gpickle("{}/{}.pkl".format(self.graph_directory, self.graph_model_name))

		# convert to neo4j query
		neo = self.pynx_to_neo4j_queries(G, return_nodes=True, return_edges=True)

		# save as txt file
		if self.gcp:
			joined_neo = "\n".join(neo)
			self.graph_bucket.blob('{}.txt'.format(self.graph_model_name)).upload_from_string(joined_neo, 'text/csv')

		else:
			with open('{}/{}.txt'.format(self.graph_directory, self.graph_model_name), 'w+') as neo_text:
				for listitem in neo:
					neo_text.write('%s\n' % listitem)

	def run_neo4j_queries(self):

		"""
		Run the neo4j queries written to the textfile corresponding to the graph_model_name
		via the self.run_imap_multiprocessing and neo_query functions. If the gcp class variable is true,
		the application will attempt to query the kubernetes neo4j server via kubectl; otherwise, 
		it will attempt to query a local neo4j server via the cypher shell. 
		"""

		if self.gcp:
			blob = self.graph_bucket.blob('{}.txt'.format(self.graph_model_name))
			neo = blob.download_as_string().decode("utf-8").split('\n')[:-1]
			os.system("kubectl exec -it neo4j-ce-1-0 -- cypher-shell -u 'neo4j' -p 'asdf' -d 'neo4j' --format plain 'MATCH (n) DETACH DELETE n'")
			print("existing graph deleted")
			# partial function sets gcp attribute
			nq = partial(neo_query, gcp=True)
			# chunksize based on previous performance
			chunksize=15
			# use the maximum number of parallel processes possible
			result_list = self.run_imap_multiprocessing(func=nq, argument_list=neo, num_processes=os.cpu_count(), chunksize=chunksize)
						

		else:	
			with open('{}/{}.txt'.format(self.graph_directory, self.graph_model_name), 'r') as file:
				neo = file.read().split('\n')[:-1]
			os.system("cypher-shell -u 'neo4j' -p 'password' --format plain 'MATCH (n) DETACH DELETE n'")	
			print("existing graph deleted")
			# partial function sets gcp attribute
			nq = partial(neo_query, gcp=True)
			# chunksize based on previous performance
			chunksize=15
			# use the maximum number of parallel processes possible
			result_list = self.run_imap_multiprocessing(func=nq, argument_list=neo, num_processes=os.cpu_count(), chunksize=chunksize)
	
	def run_imap_multiprocessing(self, func, argument_list, num_processes, chunksize=10):

		"""
		Function for mapping a function over an iterable using multiple processes. 
		:func: function to map
		:argument_list: iterable on to which we are mapping func
		:num_processes: int, number of processes to run in parallel; will be the maximum possible 
		:chunkzsize: int, the size of the chunk of the iterable that we allocate to each process
		Returns: output of func
		"""

		pool = mp.Pool(processes=num_processes)

		result_list_tqdm = []
		for result in tqdm(pool.imap(func=func, iterable=argument_list, chunksize=chunksize), total=len(argument_list)):
			result_list_tqdm.append(result)

		return result_list_tqdm			
		

	def aggregate_features(self, features_dataframe, geo_entities, aggregate_by, **feature_function_pairs):
	
		"""
		Aggregates specified features of provided dataframe by another categorical feature
		:features_dataframe: dataframe of features that will be aggregated; must have column named by 'aggregate_by'
		:geo_entities: dictionary of geographic entity shape coordinate lists, where each coordinate represents a 
		vertex of the geographic entity shape (i.e. polygon_dict).
		:aggregate_by: str, geographic entity by which to aggregate relational data. This
		will become the nodes of the graph and will be a column in the geo-tagged relational dataset.
		:**feature_function_pairs: kwargs, feature to be aggregated from relational dataset
		as the key and the aggregation function to be applied as its value
		Returns: dataframe with specified features aggregated by neighborhood and census tract
		"""

		assert(isinstance(features_dataframe, pd.core.frame.DataFrame)), "\
				features_dataframe must be a pandas dataframe"

		assert(isinstance(geo_entities, dict)),"\
			argument geo_entities must be of type dict"		

		assert(isinstance(aggregate_by, str)),"\
			argument aggregate_by must be of type str"	

		assert(aggregate_by in list(features_dataframe.columns)), "\
				{} must be a feature in features_dataframe".format(aggregate_by)
		
		# pull out names of geographic entities
		unique_geos = list(geo_entities.keys())
		# determine features to retain in dataframe
		features_to_aggregate = list(feature_function_pairs.keys())
		features = list(features_to_aggregate) + [aggregate_by]
		features_dataframe = features_dataframe[features] 

		# remove rows from the features_dataframe for which aggregate_by == 'None'
		# otherwise, you'll get a key error when trying to match up with the shapefiles later 
		if 'None' in list(features_dataframe[aggregate_by]):

			features_dataframe = features_dataframe[features_dataframe[aggregate_by] != 'None']
		
		# create new frame of just unique 'aggregate_by' values
		aggregated_frame = pd.DataFrame({aggregate_by:unique_geos})
		
		# iterate over the features to aggregate 
		"""
		current only mean and count are aggregate options 
		"""
		for feature in features_to_aggregate:
		
			if feature_function_pairs[feature] == "mean":
			
				# calculate aggregated figures for each 'aggregate_by'
				aggregated = features_dataframe.groupby(aggregate_by)\
									 [feature].mean()
			
				# determine whether there are geographic entities in the json files that are not in the
				# features_dataframe
				additionals = list(set(unique_geos) - set(list(aggregated.index)))
				for i in additionals:
					if i != np.nan:
						aggregated = aggregated.append(pd.Series({i:np.nan}, name=feature))
						aggregated = aggregated.rename_axis(aggregate_by)

			
			elif feature_function_pairs[feature] == "count":
			
				# calculate aggregated figures for each 'aggregate_by'
				aggregated = features_dataframe.groupby(aggregate_by)\
								 [feature].count()
				
				# determine whether there are geographic entities in the json files that are not in the
				# features_dataframe
				additionals = list(set(unique_geos) - set(list(aggregated.index)))
				for i in additionals:
					if i != np.nan:
						aggregated = aggregated.append(pd.Series({i:np.nan}, name=feature))
						aggregated = aggregated.rename_axis(aggregate_by)
						

			aggregated_frame = aggregated_frame.merge(aggregated, how='left', on=aggregate_by)
		
		aggregated_frame.set_index(aggregate_by, inplace=True)		
		
		return aggregated_frame
					
	def create_pynx_nodes(self, dataframe, node_category=None, attribute_columns=None, existing_graph=None):

		"""
		Transforms relational dataframe into python networkx graph.
		:frame: pandas dataframe, often the output of the aggregate_features function
		Note: dataframe indices to be used as the name of the nodes 
		:node_category: str named categorization of the node, e.g. 'neighborhood' or 'friend'
		:attribute_columns: default None, else list of attribute_columns, often a list of 
		the columns of the aggregated_features function output
		:existing_graph: default None, else existing networkx graph in memory 
		Returns: updated networkx graph structure in memory. 
		"""

		assert isinstance(dataframe, pd.core.frame.DataFrame), "\
		dataframe argument must be a pandas dataframe"

		assert isinstance(node_category, str),"\
		node_category must be string"

		if not isinstance(attribute_columns,type(None)):
			assert isinstance(attribute_columns, list),"\
			attribute_columns must be list"

		# initialize new graph
		if not existing_graph:
			G = nx.MultiDiGraph()
		# store existing networkx graph structure 
		else:
			G = existing_graph
		
		# store dataframe index is nodes 
		nodes = list(dataframe.index)

		# create nodes with defaul
		print("creating nodes")
		for i in tqdm(nodes):
			G.add_node(i, name=i, node_category=node_category)

		# create attributes if attribute_columns given
		if attribute_columns:
			print("creating attributes")
			for attribute in tqdm(attribute_columns):
				for node in nodes:
					G.nodes()[node].update({attribute:dataframe.loc[node, attribute]})

		return G

	def add_edges_to_pynx(self, graph, edge_relationship, criteria_func, criteria_func_node_pair_reference_kwargs, 
						*node_categories, bidirectional=True, **criteria_func_kwargs):

		"""
		Add edge relationships to existing pynx graph using a user-defined criteria function used to determine
		whether the stated edge relationship exists between every possible combination of nodes.
		:graph: existing python networkx graph
		:edge_relationship: str, name of the edge relationship 
		:criteria_func: user-defined criteria function, must be all kwargs, criteria function must 
		return True or a value in order for the edge relationship to be indicated
		:criteria_func_node_pair_reference_kwargs: list of key word arguments from the criteria function that 
		reference both of the node pairs (e.g. "name_1", "name_2")
		:*node_categories: node categories to be considered for edge relationships. 
		Note that if bidirectional = False and we only want to consider unidirectional edge relationships, 
		then the node category giving but not receiving the edge relationship should be listed first in the *args ***
		:bidirectional: bool, whether the edge relationship works in both directions
		:**criteria_func_kwargs: any necessary kwargs for the func; node name kwargs are already written into function
		Returns: updated graph
		
		e.g.: G = pynx_to_neo4j.add_edges_to_pynx(G, "CONTAINS", utilities.intersection, ["polygon_name_1", "polygon_name_2"], 
		"neighborhood", "census_tract", bidirectional=False, polygon_dict_1=neighborhoods, polygon_dict_2=tracts)
		"""

		assert isinstance(graph, nx.classes.multidigraph.MultiDiGraph), "\
			argument graph must be networkx multidigraph"

		assert isinstance(edge_relationship, str), "\
			argument edge_relationship must be of type string"

		# TODO: can probably simplify this component at the utilities level
		assert isinstance(criteria_func_node_pair_reference_kwargs, list), "\
			criteria_func_node_pair_reference_kwargs must be a list of kwargs for udf corresponding to the names of each\
			pair of nodes"

		assert isinstance(bidirectional, bool), "\
			argument bidirectional must be of type boolean"		

		relevant_nodes = []
		node_data = list(graph.nodes.data())

		# add only the nodes whose node_category attribute is in the *args node_category
		for i in node_data:
			if i[1]['node_category'] in node_categories:
				relevant_nodes.append(i[0])
		
		# we only want unique combinations if bidirection is True
		if bidirectional == True:

			node_combs = list(combinations(relevant_nodes, 2))

		# if the relationships aren't bidrectional, we retain only the permutations whose first node is in the first
		# node category provided in the node_categories *args
		elif bidirectional == False:			

			node_permutations = list(permutations(relevant_nodes, 2))
			first_of_the_pair = [i[0] for i in list(graph.nodes.data()) if i[1]['node_category'] == node_categories[0]]
			node_combs = [i for i in node_permutations if i[0] in first_of_the_pair and i[1] not in first_of_the_pair]

		# kwarg1 and kwarg2 is how the criteria function references the two nodes whose edge relationship it will determine
		kwarg1, kwarg2 = criteria_func_node_pair_reference_kwargs[0], criteria_func_node_pair_reference_kwargs[1]
		
		print("iterating through all possible edge relationships")
		for i in tqdm(node_combs):

			### criteria function must return True or a value in order for the edge relationship to be established ###
			if criteria_func(**{kwarg1:i[0]},
							**{kwarg2:i[1]},
							**criteria_func_kwargs):
				
				# determine whether there is an EXISTING edge relationship between these two nodes
				summy = [1 if list(graph.edges)[j][0] == i[0] and list(graph.edges)[j][1] == i[1] else 0 for j in range(len(list(graph.edges)))]
				
				# if there is:
				if sum(summy) >= 1:
					# return each one of them
					relevant_ix = [k for k,x in enumerate(summy) if x == 1]
					
					# check the nature of the existing edge relationships
					for l in relevant_ix:
						# if the existing edge relationship is the same as the one we're trying to add (e.g. 'NEXT_TO'), skip
						if list(list(graph.edges.data())[l][2].keys())[0] == edge_relationship:
							
							pass
						# otherwise, go ahead and add 
						else: 
							graph.add_edge(i[0],i[1], **{edge_relationship: {}}) 
							if bidirectional==True:
								graph.add_edge(i[1],i[0], **{edge_relationship: {}}) 
				else: 
					graph.add_edge(i[0],i[1], **{edge_relationship: {}}) 
					if bidirectional==True:
						graph.add_edge(i[1],i[0], **{edge_relationship: {}}) 

		return graph


	def pynx_to_neo4j_queries(self, graph, return_nodes=True, return_edges=True):

		"""
		Takes a python networkx format graph and outputsthe relevant queries 
		necessary to create a neo4j	representation of the graph
		:graph: pynx graph
		:return_nodes: default True, if True return neo4j queries for creating nodes
		:return_edges: default True, if True return neo4j queries for creating edges 
		Returns: list of queries
		"""

		assert isinstance(graph, nx.classes.multidigraph.MultiDiGraph), "\
			argument graph must be networkx multidigraph"

		assert isinstance(return_nodes, bool), "\
			argument return_nodes must be of type boolean"		
			
		assert isinstance(return_edges, bool), "\
			argument return_edges must be of type boolean"			

		nx_nodes = list(graph.nodes.data())
		node_names = [nx_nodes[i][0] for i in range(len(nx_nodes))]
		neo_nodes = []

		print("creating node queries")
		for i in tqdm(nx_nodes):

			n_attributes = len(list(i[1].values())) 

			# compile base information for node query
			root_info = "CREATE " + "(" + "%s" + ":" + "%s" + " {" + "%s" + ":" + '"' + "%s" + '"'
			root_info = root_info % (re.sub(r'\W+','', i[0]), i[1]['node_category'], list(i[1].keys())[0], list(i[1].values())[0].replace("'","").replace(",",""))

			end_string = "}" + ")"

			# add additional information to query based on number and nature of attributes
			additional_attributes = "" 
			for j in range(2,n_attributes):

				attribute = "," + "%s" + ":" + '"' + "%s" + '"'
				attribute = attribute % (list(i[1].keys())[j], str(list(i[1].values())[j]))
				additional_attributes += attribute 

			concatenated = root_info + additional_attributes + end_string
			neo_nodes.append(concatenated)

		nx_edges = list(graph.edges.data())
		print("creating edge queries")
		
		# compile information for edge query
		neo_edges = ['MATCH (a) WHERE a.name = "{}" MATCH (b) WHERE b.name = "{}" '\
		.format(i[0].replace("'","").replace(",",""), i[1].replace("'","").replace(",","")) + \
		"CREATE " + "(a)" + "-[:" + list(i[2].keys())[0] + " " + \
		str(list(i[2].values())[0]) + "]" + "->" + "(b)" for i in tqdm(nx_edges)]
		
		# returned entity depends on boolean function attributes 
		if return_nodes and return_edges:
			neo = neo_nodes + neo_edges
		elif return_nodes and not return_edges:
			neo = neo_nodes
		elif not return_nodes and return_edges:
			neo = neo_edges
		
		return neo 

def neo_query(query_string, gcp=False):		

	"""
	Run provided neo4j queries via kubectl or cypher shell
	:query_string: str, provided 'CREATE' neo4j query
	:gcp: bool, whether neo4j server is local or on gcp kubernetes engine
	Returns: updates to neo4j server 
	Note: We keep this outside of the class for reasons related to multiprocessing
	"""

	if gcp:
			
		os.system("kubectl exec -it neo4j-ce-1-0 -- cypher-shell -u 'neo4j' -p 'asdf' -d 'neo4j' --format plain '{}'".format(query_string))

	else:
			
		os.system("cypher-shell -u 'neo4j' -p 'password' --format plain '{}'".format(query_string))	

	
			
		
	
	
	
	
	
	


