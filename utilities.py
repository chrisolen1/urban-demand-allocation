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

def nx_to_neo_nodes(graph, return_nodes=True, return_edges=True):
    
    nx_nodes = list(graph.nodes(data=True))
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
    


    
    
    
    