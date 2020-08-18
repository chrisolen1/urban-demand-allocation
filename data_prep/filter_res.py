import argparse
parser = argparse.ArgumentParser(description="spark_res_filter_parser")
parser.add_argument("--n_workers", action="store", dest="n_workers", type=str, help="number of cores to be used for processing")
parser.add_argument("--city", action="store", dest="city", type=str, help="city of interest")
parser.add_argument("--state", action="store", dest="state", type=str, help="state of interest")
parser.add_argument("--year", action="store", dest="year", type=int, help="year of interest")

parse_results = parser.parse_args()
n_workers = parse_results.n_workers
city = parse_results.city
year = parse_results.year

import spark_filter

print("initializing spark session")
ss,sc = spark_filter.init_session(n_workers)
%time spark_filter.res_year_city_filter(ss, year, city, state)
print("closing spark session")
spark_filter.stop_session(sc)
print("session closed")


