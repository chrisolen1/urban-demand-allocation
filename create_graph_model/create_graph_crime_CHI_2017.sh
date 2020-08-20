python3 create_graph_structure.py --home_directory $HOME_DIR \
	--geo_directory $GEO_DIR --data_directory $CRIM_DIR \
	--graph_directory $GRAPH_DIR --graph_model_name test \
	--file_name  chi_crimes_standardized.csv --aggregate_function count \
	--aggregate_by neighborhood --features primary_type --gcp &

