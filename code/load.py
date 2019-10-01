import argparse
import glob

from classes.ArangoSchemaGraph import ArangoSchemaGraph
from classes.ElasticConnection import ElasticConnection
from classes.OWLLoader import OWLLoader
from classes.RDFLoader import RDFLoader
from utilities.Configuration import Configuration


def main(args):
    config = Configuration(args.config_file)
    graph = ArangoSchemaGraph(config)
    graph.init()
    a = input("Are you sure you want to load? (y/n) ")
    if a == "y":
        graph.init_indices()

        graph.load_ontologies(OWLLoader(f) if f.endswith(".owl") else RDFLoader(f, f.split(".")[-1]) for f in
                              glob.glob(config.ontology_dir + "*"))
        graph.load_mappings(config.map_file)
        graph.calculate_properties()

        ec = ElasticConnection(config.ehost, config.eport)
        index_name = config.prefix + "classes"
        if ec.index_exists(index_name):
            ec.delete_index(index_name)
        ec.add_index(index_name=index_name, collection=(d for d in graph.db.collection("classes").all()), body={
            "mappings": {"properties": {
                "label": {
                    "type": "text",
                },
                "lang": {
                    "type": "keyword"
                },
                "uri": {
                    "type": "keyword"
                },
                "equivalent": {
                    "type": "keyword"
                }
            }}
        }, overwrite=True)


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description='')
    PARSER.add_argument("-d", "--directory", type=str, help="Ontology directory", default="")
    PARSER.add_argument("-c", "--config-file", type=str, help="File with all the configurations.")
    PARSER.add_argument("-ha", "--arango-host", type=str, help="Arango address", default="")
    PARSER.add_argument("-pa", "--arango-port", type=str, help="Arango port", default="")
    PARSER.add_argument("-n", "--name", type=str, help="DB name", default="")
    PARSER.add_argument("-hes", "--elasticsearch-host", type=str, help="Elastic search address", default="")
    PARSER.add_argument("-pes", "--elasticsearch-port", type=str, help="Elastics search port", default="")
    main(PARSER.parse_args())
