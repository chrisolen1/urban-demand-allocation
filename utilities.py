import json
from shapely.geometry import shape, Point, Polygon
import re
from operator import itemgetter

"""""""""""""""""""""""""""""""""""""""""""""""""""
functions determining relationships between geographic
shape polygons, coordinate points, etc. on latitude,
longitude coordinate plane
"""""""""""""""""""""""""""""""""""""""""""""""""""

def point_lookup(polygon_dict, point):

	"""
	Assign place label corresponding to the provided
	(long, lat) or (lat, long) coordinate based on which 
	polygon from polygon_dict it fits into.
	Note: The required ordering of the coordinate tuple ((lat,long) vs. (long,lat))
	depends on the orderings present in the polygon_dict
	:polygon_dict: dictionary of geographic entity shape coordinate lists, where each coordinate represents a 
	vertex of the geographic entity shape.
	:point: tuple of (lat,long) or (long, lat) coordinates corresponding to the point of interest
	Returns: Name of the geographic entity to which the provided point corresponds. 
	The name will be one of the keys of the polygon_dict. If no match, returns "None". 
	"""

	assert(isinstance(polygon_dict, dict)), "\
			polygon_dict argument must be of type dict"

	assert(isinstance(point, tuple)), "\
			point argument must be of type tuple"
	
	# convert point to Point object
	point = Point(point) 
	
	# iterate over geographic entity keys
	for i in range(len(list(polygon_dict.keys()))):
		# Note: zero index is there because of zoning type in zoning data
		result = Polygon(polygon_dict[list(polygon_dict.keys())[i]][0]).contains(point) 
		
		# return the relevant key if we have a result
		if result == True:
			return list(polygon_dict.keys())[i]
			break

	# or else return string "None"
	return "None" 

	   
def closest_to(polygon_dict, point):

	"""
	Indicate the geographic entity closest in euclidean distance to
	the provided (lat,long) or (long,lat) tuple
	:polygon_dict: dictionary of geographic entity shape coordinate lists, where each coordinate represents a 
	vertex of the geographic entity shape.
	:point: tuple of (lat,long) or (long, lat) coordinates corresponding to the point of interest
	Returns: name of the geographic entity closest to the provided coordinate in euclidean distance 
	"""

	assert(isinstance(polygon_dict, dict)), "\
			polygon_dict argument must be of type dict"

	assert(isinstance(point, tuple)), "\
			point argument must be of type tuple"

	# convert point to Point object
	point = Point(point) 
	distances = []

	# iterate over geographic entity keys
	for i in range(len(list(polygon_dict.keys()))):
		# calculate euclidean distances
		# Note: zero index is there because of zoning type in zoning data
		dist = point.distance(Polygon(polygon_dict[list(polygon_dict.keys())[i]][0])) 
		distances.append(dist)

	# pull out and return minimum
	j, value = min(enumerate(distances), key=itemgetter(1))
	result = list(polygon_dict.keys())[j]
	
	return result


def intersection(polygon_dict_1=None, polygon_name_1=None, polygon_dict_2=None, polygon_name_2=None):

	"""
	Indicate whether two polygons overlap
	:polygon_dict_1: first dict of geographic entity shape coordinate lists, 
	where each coordinate represents a vertex of the locality shape.
	:polygon_name_1: name of a geographic entity from polygon_dict_1
	:polygon_dict_2: second dict of geographic entity shape coordinate lists, 
	where each coordinate represents a vertex of the locality shape.
	:polygon_name_2: name of a geographic entity from polygon_dict_2
	Returns: True if the two provided polygons intersection, else False 
	"""

	assert(isinstance(polygon_dict_1, dict)), "\
			polygon_dict_1 argument must be of type dict"

	assert(isinstance(polygon_dict_2, dict)), "\
			polygon_dict_2 argument must be of type dict"

	assert(isinstance(polygon_name_1, str)), "\
			polygon_name_1 argument must be of type str"

	assert(isinstance(polygon_name_2, str)), "\
			polygon_name_2 argument must be of type str"

	try:
		shape_1 = Polygon(polygon_dict_1[polygon_name_1][0]) 
	except:
		print("{} not a geographic entity in {}".format(polygon_name_1, polygon_dict_1))
	try:
		shape_2 = Polygon(polygon_dict_2[polygon_name_2][0])
	except:
		print("{} not a geographic entity in {}".format(polygon_name_2, polygon_dict_2))
	
	try:
		result = shape_1.intersects(shape_2)
	except:
		# deal with edge cases where geographic entities' boundaries overlap
		result = shape_1.buffer(0).intersects(shape_2.buffer(0)) 

	return result
	
def list_invalid_polygons(polygon_dict):

	"""
	Indicate whether any geographic entities in the provided polygon_dict
	are not valid polygons.
	:polygon_dict: dictionary of geographic entity shape coordinate lists, where each coordinate represents a 
	vertex of the geographic entity shape.
	Returns: list of invalid polygons
	"""

	names = list(polygon_dict.keys())
	invalids = []

	for i in range(len(names)):
		if not Polygon(polygon_dict[names[i]][0]).is_valid:
			invalids.append(names[i])

	return invalids



	
	