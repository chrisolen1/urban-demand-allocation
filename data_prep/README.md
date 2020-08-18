# urban-demand-allocation: data preparation

## Steps to set prep data prior to creating graph model

1. Run ```reformat_geospatial_jsons.py``` to reformat the zoning.json, neighborhood.json, and tract.json geospatial shape files:

```
python3 reformat_geospatial_jsons.py --geo_data_directory "../../data/geo_shape_files" 
```

2. Run ```spark_filter.py``` in Spark cluster to filter down original business, residential, etc. storage objects, e.g.:

```
sh shell_scripts/bus_filter_chi_2017.sh
```

3. Run ```standardize_place_names.py``` to standardize place names encoded in socioeconomic data, e.g.:

```
sh shell_scripts/standardize_chi_neighborhood_crime.sh
```


















