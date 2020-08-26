nohup python3 ../standardize_place_names.py --home_directory $HOME_DIR \
	--geo_directory $GEO_DIR --data_directory $CRIME_DIR \
	--file_name chi_crimes.csv --geo_type neighborhood \
	--n_cores 8 --gcp &
