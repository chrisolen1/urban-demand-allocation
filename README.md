# urban-demand-allocation

## Steps to setting up Chicago graph db locally:

1. Clone this repo 

2. Create directory called "data" in the same directory as the cloned github repo. It should consist of:
    a. address_book.csv
    b. chi_bus.csv
    c. crime.csv
    d properties_neighborthood_aggregated.csv
    e. properties_tract_aggregated.csv
    f. residentail_w_tract_and_neighborhoods.csv

* note that all data files live in the team Google drive folder *
  
3. Ensure that you have neo4j installed locally. Mac users can brew install:

```
brew install neo4j
```
4. Initially username should be 'neo4j'. You will also want to set neo4j password to be 'password' on the command line:

```
neo4j-admin set-initial-password password
```

5. Start your neo4j server on the command line:

```
neo4j start
```

6. Run through "create_graph.ipynb"

7. Run through "txt_to_neo4j.ipynb"

8. Open up the neo4j GUI. In your browser enter:

```
http://localhost:7474/browser/
```

9. Make sure the graph has been loaded. In the cypher command line on the neo4j GUI, enter:

```
match (a:neighborhood)-[:NEXT_TO]->(b) where a.name = "Logan Square" return a,b 
```

10. Finding average property value of all surrounding neighborhoods. Open up "neo4j_to_pandas.ipynb" and run the commands. 

11. Starting the demand model: Check out "demand_model.ipynb"













