import json
from shapely.geometry import shape, Point, Polygon
import re

# lookup up the key corresponding to the lat/long point based on what polygon it fits in

def point_lookup(polygon_dict, point):
    
    point = Point(point) # point should be a tuple
    
    for i in range(len(list(polygon_dict.keys()))):
        result = Polygon(polygon_dict[list(polygon_dict.keys())[i]][0]).contains(point) # zero index is there because of zoning type in zoning data
        
        if result == True:
            return list(polygon_dict.keys())[i]
            break 
       
       
# confirm intersection of two polygons

def intersection(polygon_dict_1, polygon_name_1, polygon_dict_2, polygon_name_2):
    
    shape_1 = Polygon(polygon_dict_1[polygon_name_1][0]) 
    shape_2 = Polygon(polygon_dict_2[polygon_name_2][0])
    
    try:
    	result = shape_1.intersects(shape_2)
    except:
    	result = shape_1.buffer(0).intersects(shape_2.buffer(0)) # deal with cases where polygons have intersecting boundaries

    return result
    
def list_invalid_polygons(polygon_dict, polygon_name):

    names = list(polygon_dict.keys())
    invalids = []

    for i in range(len(names)):
        if not Polygon(polygon_dict[names[i]][0]).is_valid:
            invalids.append(names[i])


# convert from networkx format to neo4j (not able to convert node attributes at this point):

def nx_to_neo_nodes2(graph, return_nodes=True, return_edges=True):

    nx_nodes = list(graph.nodes.data())
    neo_nodes = ["CREATE " + "(" + re.sub(r'\W+','', nx_nodes[i][0]) + ":" + list(nx_nodes[i][1].keys())[0] + " {" + list(list(nx_nodes[i][1].values())[0].keys())[0] + ":" + '"' + list(nx_nodes[i][1].values())[0]['name'] + '"' + "," + list(list(nx_nodes[i][1].values())[0].keys())[1] + ":" + '"' + str(list(nx_nodes[i][1].values())[0]['avg_property_value']) + '"' + "}" + ")" for i in range(len(nx_nodes))]
    
    nx_edges = list(graph.edges.data())
    neo_edges = ["CREATE " + "(" + re.sub(r'\W+','',nx_edges[i][0]) + ")" + "-[:" + list(nx_edges[i][2].keys())[0] + " " + str(list(nx_edges[i][2].values())[0]) + "]" + "->" + "(" + re.sub(r'\W+','',nx_edges[i][1]) + ")" for i in range(len(nx_edges))]
    
    if return_nodes and return_edges:
        neo = neo_nodes + neo_edges
        
    elif return_nodes and not return_edges:
        neo = neo_nodes
        
    elif not return_nodes and return_edges:
        neo = neo_edges    
    
    return neo
    

def nx_to_neo_nodes(graph, return_nodes=True, return_edges=True):

    n_attributes = len(list(list(list(graph.nodes.data())[0][1].values())[0].keys())) # assumes the number and types of node attributes of the first node can be generalized to the others
    
    node_attributes = list(list(list(graph.nodes.data())[0][1].values())[0].keys())

    nx_nodes = list(graph.nodes.data())
    neo_nodes = []

    

    for i in range(len(nx_nodes)):

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
    
    
    