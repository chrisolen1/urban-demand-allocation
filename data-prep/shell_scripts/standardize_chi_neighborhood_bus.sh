nohup python3 ../standardize_place_names.py --home_directory $HOME_DIR \
	--geo_directory $GEO_DIR --data_directory $BUS_DIR \
	--file_name bus_CHICAGO_2017.csv --geo_type neighborhood \
	--n_cores 8 --gcp &
