# iswc-annotation-challenge

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
  * Elasticsearch
  * ArangoDB
  
# Framework
  1. Load DBPedia into Elasticsearch ([index_dbpedia.py](https://github.com/danielapoliveira/iswc-annotation-challenge/blob/master/code/index_dbpedia.py))
  2. Load DBPedia ontology into ArangoDB ([load.py](https://github.com/danielapoliveira/iswc-annotation-challenge/blob/master/code/load.py)), including a file with mappings in the ontology
  3. Run [challenge.py](https://github.com/danielapoliveira/iswc-annotation-challenge/blob/master/code/challenge.py) with the option ```-c``` pointing to the config file
