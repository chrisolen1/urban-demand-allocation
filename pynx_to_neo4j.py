import pandas as pd
import numpy as np
import json
import re
import networkx as nx
from itertools import combinations, permutations, product

def create_pynx(frame, node_column, node_category, attribute_columns=None, existing_graph=None):

	"""
	Transforms dataframe into python networkx graph
	:frame: pandas dataframe
	:node_column: string name of column corresponding to nodes
	:node_category: string categorization of the node, e.g. 'neighborhood' or 'friend'
	:attribute_date: default None, else list of attribute_columns
	Returns: updated graph
	"""

	assert isinstance(frame, pd.core.frame.DataFrame), "\
	frame argument must be a pandas dataframe"

	assert isinstance(node_column, str),"\
	node_column must be string"

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

	nodes = list(frame[node_column])

	# create nodes with default
	for i in nodes:
		G.add_node(i, name=i, node_category=node_category)

	# create attributes
	for attribute in attribute_columns:
		for i in range(len(frame)):
			G.nodes()[frame[node_column].iloc[i]].update({attribute:frame[attribute].iloc[i]})

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
	:*node_categories: node categories to be considered for edge relationships. ***Note that if bidirectional = False and we only want to consider unidirectional
						edge relationships, then the node category giving but not receiving the edge relationship should be listed first in the *args
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
	
	for i in node_combs:

		### criteria function must return True or a value in order for the edge relationship to be established ###
		if criteria_func(**{kwarg1:i[0]},
                        **{kwarg2:i[1]},
                        **criteria_func_kwargs):
			
			graph.add_edge(i[0],i[1], **{edge_relationship: {}}) 
			if bidirectional==True:
				graph.add_edge(i[1],i[0], **{edge_relationship: {}}) 

	return graph



"""
		# dealing attributes for which we have no values
		attribute_not_in = list(set(list(G.nodes())) - set(list(frame[node_column])))
		if len(property_val_not_in) > 0:
			for i in range(len(attribute_not_in)):
			G.nodes()[attribute_not_in[i]][node_column][attribute] = str(np.NaN)
"""




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

	for i in nx_nodes:

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
	neo_edges = ["CREATE " + "(" + re.sub(r'\W+','',i[0]) + ")" + "-[:" + list(i[2].keys())[0] + " " + str(list(i[2].values())[0]) + "]" + "->" + "(" + re.sub(r'\W+','',i[1]) + ")" for i in nx_edges]
	
	if return_nodes and return_edges:
		neo = neo_nodes + neo_edges
	elif return_nodes and not return_edges:
		neo = neo_nodes
	elif not return_nodes and return_edges:
		neo = neo_edges
	return neo 


