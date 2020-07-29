import json
from shapely.geometry import shape, Point, Polygon
import re
from operator import itemgetter

def point_lookup(polygon_dict, point):

    """
    assign place label corresponding to the provided
    (long, lat) or (lat, long) coordinate based on which 
    polygon from polygon_dict it fits in
    Note: 
    """
    
    point = Point(point) # point should be a tuple
    
    for i in range(len(list(polygon_dict.keys()))):
        result = Polygon(polygon_dict[list(polygon_dict.keys())[i]][0]).contains(point) # zero index is there because of zoning type in zoning data
        
        if result == True:
            return list(polygon_dict.keys())[i]
            break

    return "None" 
       
def closest_to(polygon_dict, point):

    point = Point(point) # point should be a tuple
    distances = []

    for i in range(len(list(polygon_dict.keys()))):
        dist = point.distance(Polygon(polygon_dict[list(polygon_dict.keys())[i]][0])) # zero index is there because of zoning type in zoning data
        distances.append(dist)

    j, value = min(enumerate(distances), key=itemgetter(1))
    result = list(polygon_dict.keys())[j]
    return result

       
# confirm intersection of two polygons

def intersection(polygon_dict_1=None, polygon_name_1=None, polygon_dict_2=None, polygon_name_2=None):
    
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



    
    