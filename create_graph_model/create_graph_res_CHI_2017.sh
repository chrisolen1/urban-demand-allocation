python3 create_graph_structure.py --home_directory $HOME_DIR \
	--geo_directory $GEO_DIR --data_directory $RES_DIR \
	--graph_directory $GRAPH_DIR --graph_model_name test \
	--file_name  res_CHICAGO_2017_standardized.csv --aggregate_function mean \
	--aggregate_by neighborhood --features property_value length_of_residence &

