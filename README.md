# ADOG - Anotating Data with Ontologies and Graphs
ADOG is a system focused on leveraging the structure of a well-connected ontology graph extracted from different Knowledge Graphs to annotate structured or semi-structured data. The [Semantic Web Challenge on Tabular Data to Knowledge Graph Matching](http://www.cs.ox.ac.uk/isg/challenges/sem-tab) provided us with the means to test the system within the more restricted scenario of annotating data with a single ontology. The code hosted in this repository was used to compute the results submitted to the Round 2 of the Challenge. 

**Note:** ADOG is still in an early phase of development.

# Requirements

## Python
  * pandas 
  * tqdm 
  * inpout 
  * python-arango 
  * elasticsearch 
  * ftfy 
  * python-levenshtein 
  * rdflib 
  * owlready2
  
## Backend
  * [Elasticsearch](https://www.elastic.co/products/elasticsearch)
  * [ArangoDB](https://www.arangodb.com)
  
# Framework
  1. Load DBPedia into Elasticsearch ([index_dbpedia.py](https://github.com/danielapoliveira/iswc-annotation-challenge/blob/master/code/index_dbpedia.py))
  2. Load DBPedia ontology into ArangoDB ([load.py](https://github.com/danielapoliveira/iswc-annotation-challenge/blob/master/code/load.py)), including a file with mappings in the ontology
  3. Run [challenge.py](https://github.com/danielapoliveira/iswc-annotation-challenge/blob/master/code/challenge.py) with the option ```-c``` pointing to the config file
  
 # References
   * D. Oliveira and M. D’Aquin, “ADOG - Annotating Data with Ontologies and Graphs,” in Proceedings of the Semantic Web Challenge on Tabular Data to Knowledge Graph Matching co-located with the 18th International Semantic Web Conference, 2019, vol. 2553, p. 6. [Online]. Available: http://ceur-ws.org/Vol-2553/paper1.pdf
   * D. Oliveira, R. Sahay, and M. d’Aquin, “Leveraging Ontologies for Knowledge Graph Schemas,” in Proceedings of the 1st Workshop on Knowledge Graph Building co-located with ESWC 2019, Portoroz, Slovenia, 2019, vol. 2489, pp. 24–36. [Online]. Available: http://ceur-ws.org/Vol-2489/paper3.pdf

