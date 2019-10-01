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
   * Daniela Oliveira and Mathieu d'Aquin. [ADOG - Anotating Data with Ontologies and Graphs.](http://www.cs.ox.ac.uk/isg/challenges/sem-tab/papers/ADOG.pdf)
   * Daniela Oliveira, Ratnesh Sahay, Mathieu d'Aquin. [Leveraging Ontologies for Knowledge Graph Schemas.](https://openreview.net/pdf?id=B1xnsmvaUE)
