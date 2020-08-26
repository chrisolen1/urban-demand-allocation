import pandas as pd
import numpy as np
import json

import os
import sys

import networkx as nx
from py2neo import Graph

import utilities

class graph_model(object):

	def __init__(self, home_directory=home_directory,
			geo_directory=geo_directory,
			graph_directory=graph_directory,
			graph_model_name=graph_model_name,
			gcp=gcp):

	self.home_directory = home_directory
	self.geo_directory = geo_directory
	self.graph_directory = graph_directory
	self.graph_model_name = graph_model_name
	self.gcp = gcp

	sys.path.append(self.home_directory)

	if gcp:
		import gcsfs
    	from google.cloud import storage
    	storage_client = storage.Client()
		self.geo_bucket = storage_client.get_bucket(geo_directory[5:])
		self.graph_bucket = storage_client.get_bucket(graph_directory[5:])


	def create_structure(self, *features_to_aggregate
						aggregate_by=aggregate_by,
						data_directory=data_directory,
						file_name=file_name,
						aggregate_function=aggregate_function):

		if gcp:
			blob = self.geo_bucket.blob('chicago_{}_reformatted.json'.format(aggregate_by))
			geo = json.loads(blob.download_as_string(client=None))
		else:
			with open('{}/chicago_{}_reformatted.json'.format(self.geo_directory, aggregate_by),'r') as f:
        		geo = json.load(f)

        df = pd.read_csv('{}/{}'.format(data_directory, file_name))
        df_aggregated = aggregate_features(df, aggregate_function, geo, aggregate_by, *features_to_aggregate)

        # determine whether a serialized version of this graph_model already exists 
        exists = ""
		if self.gcp:
    		graph_list = list(self.graph_bucket.list_blobs())
    		result = [1 if self.graph_model_name in str(name) else 0 for name in graph_list]
    		if sum(result) == 1:
        		exists = True
        		ix = result.index(1)
    		else:
        		exists = False

		else:
    		graph_list = os.listdir(self.graph_directory)
    		result = [1 if self.graph_model_name in name else 0 for name in graph_list]
    		if sum(result) == 1:
        		exists = True
        		ix = result.index(1)
    		else:
        		exists = False


		if not exists:
    
    		# create nodes named after 'aggregate_by' with property value attributes corresponding to df
    		# e.g.: neighborhoods, chicago, residential
    		G = create_pynx_nodes(df_aggregated, node_category=aggregate_by,
                                    attribute_columns=list(df_aggregated.columns))

    		# create neighborhood to neighborhood edges
    		G = pynx_to_neo4j.add_edges_to_pynx(G, "NEXT_TO", utilities.intersection, ["polygon_name_1", "polygon_name_2"], \
                                    aggregate_by ,bidirectional=True, polygon_dict_1=geo, \
                                    polygon_dict_2=geo)

		if exists:

		    print("adding to existing graph")
    		if self.gcp:
        
        		blob = self.graph_bucket.blob('{}.pkl'.format(self.graph_model_name))
        		blob.download_to_filename("{}/create_graph_model/temp/temp.pkl".format(self.home_directory), client=None)
        		G = nx.read_gpickle("{}/create_graph_model/temp/temp.pkl".format(self.home_directory))
        		os.remove("{}/create_graph_model/temp/temp.pkl".format(self.home_directory))
    
    		else:
        
        		G = nx.read_gpickle("{}/{}.pkl".format(self.graph_directory, self.graph_model_name))

    		# create nodes named after 'aggregate_by' with property value attributes corresponding to df
    		# e.g.: neighborhoods, chicago, residential
    		G = create_pynx_nodes(df_aggregated, node_category=aggregate_by, 
                                    attribute_columns=list(df_aggregated.columns), existing_graph=G)

    		# create neighborhood to neighborhood edges
    		G = add_edges_to_pynx(G, "NEXT_TO", utilities.intersection, ["polygon_name_1", "polygon_name_2"], \
                                    aggregate_by ,bidirectional=True, polygon_dict_1=geo, \
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
    		nx.write_gpickle(G, "{}/create_graph_model/temp/{}.pkl".format(self.home_directory, self.graph_model_name))
    		# then upload to bucket
    		blob = self.graph_bucket.blob('{}.pkl'.format(self.graph_model_name))
    		blob.upload_from_filename("{}/create_graph_model/temp/{}.pkl".format(self.home_directory, self.graph_model_name))
    		os.remove("{}/create_graph_model/temp/{}.pkl".format(self.home_directory, self.graph_model_name))
    
		else:
    		nx.write_gpickle(G, "{}/{}.pkl".format(self.graph_directory, self.graph_model_name))

    def create_neo4j_queries(self):

    	if self.gcp:
    		blob = self.graph_bucket.blob('{}.pkl'.format(self.graph_model_name))
    		blob.download_to_filename("{}/create_graph_model/temp/temp.pkl".format(self.home_directory), client=None)
    		G = nx.read_gpickle("{}/create_graph_model/temp/temp.pkl".format(self.home_directory))
    		os.remove("{}/create_graph_model/temp/temp.pkl".format(self.home_directory))

		else:
			G = nx.read_gpickle("{}/{}.pkl".format(self.graph_directory, self.graph_model_name))

		# convert to neo4j query
		neo = pynx_to_neo4j_queries(G, return_nodes=True, return_edges=True)

		# save as txt file
		if self.gcp:
			joined_neo = "\n".join(neo)
			self.graph_bucket.blob('{}.txt'.format(self.graph_model_name)).upload_from_string(joined_neo, 'text/csv')

		else:
			with open('{}/{}.txt'.format(self.graph_directory, self.graph_model_name), 'w') as neo_text:
				for listitem in neo:
					neo_text.write('%s\n' % listitem)

	def query_neo4j(self):

		if self.gcp:
    		blob = self.graph_bucket.blob('{}.pkl'.format(self.graph_model_name))
    		neo = blob.download_as_string().decode("utf-8").replace('\n', ' \n')

    		# Create a node via cypher-shell
			#kubectl exec -it "${APP_INSTANCE_NAME}-0" --namespace "${NAMESPACE}" -- cypher-shell -u "neo4j" -p "${NEO4J_PASSWORD}" -d "neo4j" 'CREATE(n:Person { name: "John Doe"})'

		else:	
			with open('{}/{}.txt'.format(self.graph_directory, self.graph_model_name), 'r') as file:
    			neo = file.read().replace('\n', ' \n')

			# establish connection to neo4j
			graph = Graph("bolt://localhost:7687", user="neo4j", password="password")
			print("connected to neo4j server")

			# delete existing graph if one exists
			trans_action = graph.begin()
			statement = "MATCH (n) DETACH DELETE n"
			trans_action.run(statement)
			trans_action.commit()
			print("existing schema deleted")

			# run queries
			trans_action = graph.begin()
			statement = neo
			trans_action.run(neo)
			trans_action.commit()
			print("new schema created")




def aggregate_features(features_dataframe, aggregate_function, geo_shape_file, aggregate_by, *features_to_aggregate):
	
	"""
	Takes a a pandas dataframe of socioeconomic data (e.g. crime, property values)
	and returns a new frame aggregated by census tract and neighborhood
	:features_dataframe: socioeconomic features; must have 'tract' and 'neighborhood' column
	:aggregate_function: str, must be either 'count' or 'mean'
	:geo_data_directory: directory location of the geo shapefiles
	:*features_to_aggregate: args, series of features from the dataframe that you want aggregated
	Returns: dataframe with specified features aggregated by neighborhood and census tract
	"""

	assert(isinstance(features_dataframe, pd.core.frame.DataFrame)), "\
			features_dataframe must be a pandas dataframe"

	assert(aggregate_function == "count" or aggregate_function == "mean"), "\
			aggregate function must be either count or mean"
	
	assert(aggregate_by == "neighborhood" or aggregate_by == "tract"), "\
			aggregate_by must be either neighborhood or tract"


	unique_geos = list(geo_shape_file.keys())
	# determine features to retain in dataframe
	features = list(features_to_aggregate) + [aggregate_by]
	features_dataframe = features_dataframe[features] 

	# remove rows from the socioeconomic data for which 'neighborhood' or 'tract' == 'None'
	# otherwise, you'll get a key error when trying to match up with the shapefiles later 
	if 'None' in list(features_dataframe[aggregate_by]):

		features_dataframe = features_dataframe[features_dataframe[aggregate_by] != 'None']
			   
	if aggregate_function == "mean":
		
		# calculate aggregated figures for each neighborhood
		aggregated = features_dataframe.groupby(aggregate_by)\
							 [list(features_to_aggregate)].mean()
		# determine whether there are geographic entities in the json files that are not in the
		# socioeconomic data
		additionals = list(set(unique_geos) - set(list(aggregated.index)))
		for i in additionals:
			if i != np.nan:
				aggregated = aggregated.append(pd.Series(name=i))
		
	elif aggregate_function == "count":
		
		# calculate aggregated figures for each neighborhood
		aggregated = features_dataframe.groupby(aggregate_by)\
							 [list(features_to_aggregate)].count()
		# determine whether there are geographic entities in the json files that are not in the
		# socioeconomic data
		additionals = list(set(unique_geos) - set(list(aggregated.index)))
		for i in additionals:
			if i != np.nan:
				aggregated = aggregated.append(pd.Series(name=i))
		
	return aggregated
				
def create_pynx_nodes(frame, node_category=None, attribute_columns=None, existing_graph=None):

	"""
	Transforms dataframe into python networkx graph
	:frame: pandas dataframe
	:node_category: string categorization of the node, e.g. 'neighborhood' or 'friend'
	:attribute_date: default None, else list of attribute_columns
	Returns: updated graph
	"""

	assert isinstance(frame, pd.core.frame.DataFrame), "\
	frame argument must be a pandas dataframe"

	assert isinstance(node_category, str),"\
	node_category must be string"

	if not isinstance(attribute_columns,type(None)):
		assert isinstance(attribute_columns, list),"\
		attribute_columns must be list"

	# initialize new graph
	if not existing_graph:
		G = nx.MultiDiGraph()
	else:
		G = existing_graph

	nodes = list(frame.index)

	# create nodes with default
	print("creating nodes")
	for i in tqdm(nodes):
		G.add_node(i, name=i, node_category=node_category)

	# create attributes if attribute_columns given
	if attribute_columns:
		print("creating attributes")
		for attribute in tqdm(attribute_columns):
			for node in nodes:
				G.nodes()[node].update({attribute:frame.loc[node, attribute]})

	return G

def add_edges_to_pynx(graph, edge_relationship, criteria_func, criteria_func_node_pair_reference_kwargs, *node_categories, bidirectional=True, **criteria_func_kwargs):

	"""
	Add edge relationships to existing pynx
	graph using a user-defined criteria function used to determine
	whether the stated edge relationship exists between every possible
	combination of nodes
	:graph: existing pynx graph
	:edge_relationship: str, name of the edge relationship 
	:criteria_func: user-defined criteria function, must be all kwargs, criteria function must return True or a value in order for the edge relationship to be established
	:criteria_func_node_pair_reference_kwargs: key word arguments from the criteria function that reference both of the node pairs (e.g. "name_1", "name_2")
	:*node_categories: node categories to be considered for edge relationships. 
						***Note that if bidirectional = False and we only want to consider unidirectional edge relationships, 
						then the node category giving but not receiving the edge relationship should be listed first in the *args ***
	:bidirectional: bool, whether the edge relationship works in both directions
	:**criteria_func_kwargs: any necessary kwargs for the func; node name kwargs are already written into function
	Returns: updated graph
	
	e.g.: G = pynx_to_neo4j.add_edges_to_pynx(G, "CONTAINS", utilities.intersection, ["polygon_name_1", "polygon_name_2"], 
	"neighborhood", "census_tract", bidirectional=False, polygon_dict_1=neighborhoods, polygon_dict_2=tracts)
	"""

	assert isinstance(graph, nx.classes.multidigraph.MultiDiGraph), "\
	graph must be networkx multidigraph"

	assert isinstance(edge_relationship, str), "\
	edge_relationship must be string"

	assert isinstance(criteria_func_node_pair_reference_kwargs, list), "\
	criteria_func_node_pair_reference_kwargs must be a list of kwargs for udf corresponding to the names of each\
	pair of nodes"

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

	#kwarg1 and kwarg2 is how the criteria function references the two nodes whose edge relationship it will determine
	kwarg1, kwarg2 = criteria_func_node_pair_reference_kwargs[0], criteria_func_node_pair_reference_kwargs[1]
	
	print("iterating through all possible edge relationships")
	for i in tqdm(node_combs):

		### criteria function must return True or a value in order for the edge relationship to be established ###
		if criteria_func(**{kwarg1:i[0]},
                        **{kwarg2:i[1]},
                        **criteria_func_kwargs):
			
			graph.add_edge(i[0],i[1], **{edge_relationship: {}}) 
			if bidirectional==True:
				graph.add_edge(i[1],i[0], **{edge_relationship: {}}) 

	return graph


def pynx_to_neo4j_queries(graph, return_nodes=True, return_edges=True):

	"""
	Takes a python networkx format graph and outputs
	the relevant queries necessary to create a neo4j
	representation of said graph
	:graph: pynx graph
	:return_nodes: default True, if True return neo4j queries for creating nodes
	:return_edges: default True, if True return neo4j queries for creating edges 
	Returns: list of queries
	"""

	nx_nodes = list(graph.nodes.data())
	neo_nodes = []

	print("creating node queries")
	for i in tqdm(nx_nodes):

		n_attributes = len(list(i[1].values())) 

		root_info = "CREATE " + "(" + "%s" + ":" + "%s" + " {" + "%s" + ":" + '"' + "%s" + '"'
		root_info = root_info % (re.sub(r'\W+','', i[0]), i[1]['node_category'], list(i[1].keys())[0], list(i[1].values())[0])

		end_string = "}" + ")"

		additional_attributes = "" 
		for j in range(2,n_attributes):

			attribute = "," + "%s" + ":" + '"' + "%s" + '"'
			attribute = attribute % (list(i[1].keys())[j], str(list(i[1].values())[j]))
			additional_attributes += attribute 

		concatenated = root_info + additional_attributes + end_string
		neo_nodes.append(concatenated)

	nx_edges = list(graph.edges.data())
	print("creating edge queries")
	neo_edges = ["CREATE " + "(" + re.sub(r'\W+','',i[0]) + ")" + "-[:" + list(i[2].keys())[0] + " " + str(list(i[2].values())[0]) + "]" + "->" + "(" + re.sub(r'\W+','',i[1]) + ")" for i in tqdm(nx_edges)]
	
	if return_nodes and return_edges:
		neo = neo_nodes + neo_edges
	elif return_nodes and not return_edges:
		neo = neo_nodes
	elif not return_nodes and return_edges:
		neo = neo_edges
	return neo 


	
			
		
	
	
	
	
	
	

