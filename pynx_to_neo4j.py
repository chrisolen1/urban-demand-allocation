import pandas as pd
import numpy as np
import json
import re
import networkx as nx
from itertools import combinations, product

def create_pynx(frame, node_column, attribute_columns=None, existing_graph=None):

	"""
	Transforms dataframe into python networkx graph
	:frame: pandas dataframe
	:node_column: string name of column corresponding to nodes
	:attribute_date: default None, else list of attribute_columns
	Returns: updated graph
	"""

	assert isinstance(frame, pd.core.frame.DataFrame), "\
	frame attribute must be a pandas dataframe"

	assert isinstance(node_column, str),"\
	node_column must be string"

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
		G.add_node(i, name=i)

	# create attributes
	for attribute in attribute_columns:
		for i in range(len(frame)):
			G.nodes()[frame[node_column].iloc[i]].update({attribute:frame[attribute].iloc[i]})

	return G

def add_edges_to_pynx(graph, edge_relationship, criteria_func, node_kwarg_names, bidirectional=True, **criteria_func_kwargs):

	"""
	Add edge relationships to existing pynx
	graph using a user-defined criteria function used to determine
	whether the stated edge relationship exists between every possible
	combination of nodes
	:graph: existing pynx graph
	:edge_relationship: str, name of the edge relationship 
	:criteria_func: user-defined criteria function, must be all kwargs
	:bidirectional: bool, whether the edge relationship works in both directionds
	:**criteria_func_kwargs: any necessary kwargs for the func; node name kwargs are already written into function
	Returns: updated graph
	
	e.g.: G = _add_edges_to_pynx(G, "NEXT_TO", utilities.intersection, ["polygon_name_1", "polygon_name_2"],
	bidirectional=True, polygon_dict_1=neighborhoods, polygon_dict_2=neighborhoods)
	"""

	assert isinstance(graph, nx.classes.multidigraph.MultiDiGraph), "\
	graph must be networkx multidigraph"

	assert isinstance(edge_relationship, str), "\
	edge_relationship must be string"

	assert isinstance(node_kwarg_names, list), "\
	node_kwarg_names must be a list of kwargs for udf corresponding to the names of each\
	pair of nodes"

	nodes = list(graph.nodes())
	node_combinations = list(combinations(nodes, 2))

	kwarg1, kwarg2 = node_kwarg_names[0], node_kwarg_names[1]
	
	for i in range(len(node_combinations)):

		if criteria_func(**{kwarg1:node_combinations[i][0]},
                        **{kwarg2:node_combinations[i][1]},
                        **criteria_func_kwargs):
			
			graph.add_edge(node_combinations[i][0],node_combinations[i][1], **{edge_relationship: {}}) 
			if bidirectional==True:
				graph.add_edge(node_combinations[i][1],node_combinations[i][0], **{edge_relationship: {}}) 

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

	for i in range(len(nx_nodes)):

		n_attributes = len(list(list(list(graph.nodes.data())[i][1].values())[0].keys())) 
		node_attributes = list(list(list(graph.nodes.data())[i][1].values())[0].keys())

		root_info = "CREATE " + "(" + "%s" + ":" + "%s" + " {" + "%s" + ":" + '"' + "%s" + '"'
		root_info = root_info % (re.sub(r'\W+','', nx_nodes[i][0]), list(nx_nodes[i][1].keys())[0], list(list(nx_nodes[i][1].values())[0].keys())[0], list(nx_nodes[i][1].values())[0]['name'])

		end_string = "}" + ")"

		additional_attributes = "" 
		for j in range(1,n_attributes):

			attribute = "," + "%s" + ":" + '"' + "%s" + '"'
			attribute = attribute % (list(list(nx_nodes[i][1].values())[0].keys())[j], str(list(nx_nodes[i][1].values())[0][node_attributes[j]]))
			additional_attributes += attribute 

		concatenated = root_info + additional_attributes + end_string
		neo_nodes.append(concatenated)

	nx_edges = list(graph.edges.data())
	neo_edges = ["CREATE " + "(" + re.sub(r'\W+','',nx_edges[i][0]) + ")" + "-[:" + list(nx_edges[i][2].keys())[0] + " " + str(list(nx_edges[i][2].values())[0]) + "]" + "->" + "(" + re.sub(r'\W+','',nx_edges[i][1]) + ")" for i in range(len(nx_edges))]
	
	if return_nodes and return_edges:
		neo = neo_nodes + neo_edges
	elif return_nodes and not return_edges:
		neo = neo_nodes
	elif not return_nodes and return_edges:
		neo = neo_edges
	return neo 


