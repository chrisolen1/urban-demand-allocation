import json
from shapely.geometry import shape, Point, Polygon

"""
reformat geospatial json files to extract
place names and their respective geospatial coordinates
"""

# clean neighborhoods json file:

with open('../dicts/neighborhoods.json','r') as f:
    neighborhoods_dict = json.load(f)

neighborhoods = [neighborhoods_dict['data'][i][8:10] for i in range(len(neighborhoods_dict['data']))]

names = [neighborhoods_dict['data'][i][9] for i in range(len(neighborhoods_dict['data']))]
polygons = [neighborhoods_dict['data'][i][8] for i in range(len(neighborhoods_dict['data']))]

neighborhoods = {k:v for (k,v) in zip(names,polygons)}

polys_split = [neighborhoods[list(neighborhoods.keys())[i]].split(",") for i in range(len(neighborhoods.keys()))]
polys_split = [[j.replace("MULTIPOLYGON","").replace("(","").replace(")","") for j in polys_split[i]] for i in range(len(polys_split))]
polys_split = [[j.lstrip() for j in polys_split[i]] for i in range(len(polys_split))]
polys_split = [[j.split(" ") for j in polys_split[i]] for i in range(len(polys_split))]
polys_split = [[[float(k) for k in polys_split[i][j]] for j in range(len(polys_split[i]))] for i in range(len(polys_split))]

neighborhoods = {k:[v] for (k,v) in zip(names,polys_split)}

with open ("../dicts/neighborhood_polys.json",'w') as f:
    json.dump(neighborhoods, f)

# clean zoning json file:

with open('../dicts/zoning.json','r') as f:
    zoning_dict = json.load(f)

names = [zoning_dict['data'][i][0] for i in range(len(zoning_dict['data']))]
polygons = [zoning_dict['data'][i][8] for i in range(len(zoning_dict['data']))]
zoning_type = [zoning_dict['data'][i][13] for i in range(len(zoning_dict['data']))]

zoning = {k:v for (k,v) in zip(names,polygons)}

polys_split = [zoning[list(zoning.keys())[i]].split(",") for i in range(len(zoning.keys()))]
polys_split = [[j.replace("MULTIPOLYGON","").replace("(","").replace(")","") for j in polys_split[i]] for i in range(len(polys_split))]
polys_split = [[j.lstrip() for j in polys_split[i]] for i in range(len(polys_split))]
polys_split = [[j.split(" ") for j in polys_split[i]] for i in range(len(polys_split))]
polys_split = [[[float(k) for k in polys_split[i][j]] for j in range(len(polys_split[i]))] for i in range(len(polys_split))]

zoning = {k:v for (k,v) in zip(names,polys_split)}

for i in range(len(zoning.keys())):
    zoning.update({list(zoning.keys())[i]:[zoning[list(zoning.keys())[i]],zoning_type[i]]})

with open ("../dicts/zoning_polys.json",'w') as f:
    json.dump(zoning, f)

# cleaning census track json file:

with open('../dicts/tracts.json','r') as f:
    tract_dict = json.load(f)

names = ["ct_"+tract_dict['data'][i][13] for i in range(len(tract_dict['data']))]
polygons = [tract_dict['data'][i][8] for i in range(len(tract_dict['data']))]

tracts = {k:v for (k,v) in zip(names,polygons)}

polys_split = [tracts[list(tracts.keys())[i]].split(",") for i in range(len(tracts.keys()))]
polys_split = [[j.replace("MULTIPOLYGON","").replace("(","").replace(")","") for j in polys_split[i]] for i in range(len(polys_split))]
polys_split = [[j.lstrip() for j in polys_split[i]] for i in range(len(polys_split))]
polys_split = [[j.split(" ") for j in polys_split[i]] for i in range(len(polys_split))]
polys_split = [[[float(k) for k in polys_split[i][j]] for j in range(len(polys_split[i]))] for i in range(len(polys_split))]

tracts = {k:[v] for (k,v) in zip(names,polys_split)}

with open ("../dicts/tract_polys.json",'w') as f:
    json.dump(tracts, f)


















