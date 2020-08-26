# urban-demand-allocation: graph model

## Steps to set up graph model

1. Run ```python3 create_graph_model.py``` to aggregate socioeconomic data by neighborhood and census tract and output the queries required to create the urban graph model.

2. Run ```python3 txt_to_neo4j.py``` to execute neo4j queries in neo4j server.
  
## Running Neo4j locally

1. Ensure that you have neo4j installed locally. Mac users can brew install:

```
brew install neo4j
```

2. Initially username should be 'neo4j'. You will also want to set neo4j password to be 'password' on the command line:

```
neo4j-admin set-initial-password password
```

3. Start your neo4j server on the command line:

```
neo4j start
```

4. To open up the neo4j GUI, in your browser enter:

```
http://localhost:7474/browser/
```

5. Make sure the graph has been loaded. In the cypher command line on the neo4j GUI, enter:

```
match (a:neighborhood)-[:NEXT_TO]->(b) where a.name = "Logan Square" return a,b 
```















