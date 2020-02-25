import json
from shapely.geometry import shape, Point, Polygon

# lookup up the key corresponding to the lat/long point based on what polygon it fits in

def point_lookup(polygon_dict, point):
    
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




    
    
    
    