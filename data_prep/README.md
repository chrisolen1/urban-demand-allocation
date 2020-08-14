# urban-demand-allocation: data preparation

## Steps to set prep data prior to creating graph model

1. Run ```reformat_geospatial_jsons.py``` to reformat the zoning.json, neighborhood.json, and tract.json geospatial shape files:

```
python3 reformat_geospatial_jsons.py --geo_data_directory "../../data/geo_shape_files" 
```

2. Run ```standardize_place_names.py``` to standardize place names encoded in socioeconomic data. 

```
python3 standardize_place_names.py --geo_data_directory "../../data/geo_shape_files" --data_directory "../../data"
```
















