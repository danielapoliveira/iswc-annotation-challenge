import math
import time
from collections import defaultdict
from multiprocessing.pool import Pool

from arango import ArangoClient, VertexCollectionDeleteError, EdgeDefinitionDeleteError
from tqdm import tqdm

from utilities import settings
from utilities.general import chunks


class ArangoSchemaGraph:
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.host = config.ahost
        self.port = config.aport
        self.name = config.adb

        self.client = None
        self.sys_db = None
        self.db = None
        self.graph = None
        self._open()
        self._diameter = 0
        self._ontology = {}
        self._uri_map = {}
        self._properties = {}
        self._edges_to = {}
        self._edges_from = {}
        self._parents = defaultdict(list)
        self._children = defaultdict(list)
        self._object_properties = defaultdict(list)
        self._depth_types = {}

    def init(self):
        if self.sys_db.has_database(self.name):
            self.sys_db.delete_database(self.name)
            self.sys_db.create_database(self.name)
            self.db = self.client.db(self.name)
        else:
            self.sys_db.create_database(self.name)
            self.db = self.client.db(self.name)

    def init_graph(self):
        if self.db.has_graph("schema-graph"):
            self.graph = self.db.graph("schema-graph")
        else:

            print("Graph doesn't exist.")

    def calculate_properties(self):
        if self.db.has_collection("properties"):
            p = self.db.collection("properties")
        else:
            p = self.db.create_collection("properties")

        print("Calculating depth")
        if p.has("max_depth"):
            p.delete("max_depth")
        p.insert({
            "_key": "max_depth",
            "value": self.calculate_max_depth()
        })

        if p.has("diameter"):
            p.delete("diameter")
        p.insert({
            "_key": "diameter",
            "value": self._calculate_diameter()
        })

    def load_ontologies(self, ontologies):
        classes = []
        subclass_edges = []
        objectprop_edges = []

        for ontology in ontologies:
            for oclass in ontology.get_classes():
                doc = self._create_class_document(oclass, ontology)
                for equiv in oclass.equivalent_to:
                    if "http://dbpedia.org/ontology/" in equiv.iri:
                        doc["equivalent"] = equiv.iri
                if doc is not None:
                    classes.append(doc)
            self.db.collection("classes").import_bulk(classes)
            for oclass in ontology.get_classes():
                for subclass in ontology.subclasses(oclass):
                    doc = self._create_edge_document(subclass, oclass, label="subclassof", weight=0.5,
                                                     ontology=ontology)
                    if doc is not None:
                        subclass_edges.append(doc)
            self.db.collection("subclassof").import_bulk(subclass_edges)
            for op in ontology.get_object_properties():
                pdomain = op.domain
                prange = op.range
                if pdomain and prange:
                    for d in pdomain:
                        for r in prange:
                            if d is not None and r is not None:
                                for l in op.label:
                                    doc = self._create_edge_document(d, r, label=l, weight=1.0, ontology=ontology,
                                                                     uri=ontology.get_iri(op), lang=l.lang)
                                    if doc is not None:
                                        objectprop_edges.append(doc)
            self.db.collection("objectproperties").import_bulk(objectprop_edges)

    def load_mappings(self, filename):
        mappings = []
        print("Adding mappings...")
        with open(filename) as f:
            for line in f:
                line = line.split("\t")
                try:
                    source = self.get_document(self.get_document_from_uri(line[0]))
                    target = self.get_document(self.get_document_from_uri(line[2]))
                    if source is not None and target is not None:
                        mappings.append({
                            '_from': source["_id"],
                            '_to': target["_id"],
                            'label': "mapping",
                            'weight': 0.8
                        })
                except KeyError:
                    pass
        self.db.collection("classmappings").import_bulk(mappings)

    def _create_class_document(self, entity, ontology):
        # TODO: check if it's necessary to keep the information about the ontologies that contain this class with
        #  'ontologies': [ontology.get_ontology_iri()]
        uri = ontology.get_iri(entity)
        if "http://dbpedia.org/ontology/" in uri:
            return {'uri': uri,
                    'label': ontology.get_label(entity),
                    'definition': ontology.get_definition(entity)
                    }

    def _create_edge_document(self, source, target, label, weight, ontology, lang="", uri=""):
        if "http://dbpedia.org/ontology/" in source.iri and "http://dbpedia.org/ontology/" in target.iri:
            return {
                '_from': self.uris()[source.iri],
                '_to': self.uris()[target.iri],
                'label': label,
                'language': lang,
                "uri": uri,
                'weight': weight
            }

    def all_collections(self):
        return (c["name"] for c in self.db.collections() if not c["system"])

    def get_document(self, doc_id):
        if not self._ontology:
            self._ontology = self.ontology()
        return self._ontology[doc_id]

    def get_uri_from_document(self, doc_id):
        return self.get_document(doc_id)["uri"]

    def get_object_property(self, source, target):
        s_id = self.get_document_from_uri(source, "classes")["_id"]
        t_id = self.get_document_from_uri(target, "classes")["_id"]
        edges = self.graph.edge_collection("objectproperties").edges(s_id, direction="out")["edges"]
        for each in edges:
            if t_id in each["_to"]:
                print(source, target, each)

    def get_document_from_uri(self, uri):
        if uri in self.uris():
            return self.uris()[uri]
        else:
            return None

    @staticmethod
    def get_path(doc_id, d):
        queue = [doc_id]
        tree = set()
        while queue:
            current = queue.pop()
            superclass = d[current]
            if len(superclass) == 1:
                queue.append(superclass[0])
                tree.add(superclass[0])
            elif len(superclass) != 0:
                print("more than one parent")

        return tree

    def get_subclasses(self, doc_id):
        if not self._parents:
            self.hierarchy()
        return self.get_path(doc_id, self._parents)

    def get_superclasses(self, doc_id, return_uri=False):
        if "http://dbpedia.org/ontology/" in doc_id:
            doc_id = self.get_document_from_uri(doc_id)
        if not self._children:
            self.hierarchy()
        parents = self.get_path(doc_id, self._children)
        if not return_uri:
            return parents
        else:
            return set(self.get_uri_from_document(parent_id) for parent_id in parents)

    def get_graph_name(self):
        return self.graph.name

    def get_diameter(self):
        if self._diameter != 0:
            return self._diameter
        else:
            self._diameter = self.properties()["diameter"]
            return self._diameter

    def get_tree(self, _id, distance):
        self.hierarchy()
        queue = [[_id]]
        position = 0
        tree = set()
        while queue and position <= distance:
            current = queue.pop()
            # print(current)
            this_level = []
            for each in current:
                if each in self._parents:
                    parent = self._parents[each]
                    this_level.extend(parent)
                    tree.update(parent)
                if each in self._children:
                    child = self._children[each]
                    this_level.extend(child)
                    tree.update(child)

            queue.append(this_level)
            position += 1
        return tree

    def get_depths(self, uri):
        if self._depth_types:
            return self._depth_types[uri]
        else:
            self.depths()

    def depths(self):
        if not self._depth_types:
            self._depth_types = {c["uri"]: c["depth"] for c in self.db.collection("classes").all().batch()}
        return self._depth_types

    def ontology(self):
        if self._ontology:
            return self._ontology
        else:
            self._ontology = {c["_id"]: c for c in self.db.collection("classes").all().batch()}
            return self._ontology

    def hierarchy(self):
        if not self._parents:
            for c in self.db.collection("subclassof").all().batch():
                self._parents[c["_to"]].append(c["_from"])
                self._children[c["_from"]].append(c["_to"])
        return self._parents, self._children

    def object_properties(self):
        if not self._object_properties:
            for c in self.db.collection("objectproperties").all().batch():
                self._object_properties[c["_from"]].append(c["_to"])
        return self._object_properties

    def uris(self):
        if not self._uri_map:
            for _id, doc in self.ontology().items():
                self._uri_map[doc["uri"]] = _id
        return self._uri_map

    def properties(self):
        if not self._properties:
            for c in self.db.collection("properties").all().batch():
                self._properties[c["_key"]] = c["value"]
        return self._properties

    def calculate_idf(self):
        N = self.properties()["types_count"]
        classes = self.db.collection("classes")
        max_idf = 0
        for c in tqdm(classes.all().batch(), total=classes.count(), desc="Documents"):
            if "typeidf" not in c:
                cid = c["uri"]
                query = f"FOR doc IN types FILTER @cid IN doc.type[*] RETURN doc"
                doc = tuple(x for x in self.db.aql.execute(query, bind_vars={"cid": cid}))
                idf = math.log(N / (1 + len(doc)))
                c["typeidf"] = idf
                classes.update(c)
            else:
                idf = c["typeidf"]

            if idf > max_idf:
                max_idf = idf

        p = self.db.collection("properties")
        if p.has("max_idf"):
            p.delete("max_idf")
        p.insert({
            "_key": "max_idf",
            "value": max_idf
        })

    def get_idf(self, term_uri):
        if "http://dbpedia.org/ontology/" not in term_uri:
            doc = self.ontology()[term_uri]
        else:
            doc = self.ontology()[self.uris()[term_uri]]
        if "typeidf" in doc:
            return doc["typeidf"]
        else:
            return 0

    def get_bottom_up_depth(self, start):
        max_depth = 0
        path_len = 0
        stack, path = [start], []
        while stack:
            vertex = stack.pop(0)
            if vertex in path:
                continue
            path.append(vertex)
            superclasses = self.get_superclasses(vertex)
            if self.get_uri_from_document(vertex) == "http://dbpedia.org/ontology/AmusementParkAttraction":
                print(superclasses)
            if not superclasses or superclasses is None:
                path_len += 1
                if path_len > max_depth:
                    max_depth = path_len
                path = []
            else:
                for neighbor in superclasses:
                    stack.append(neighbor)
        return max_depth

    def calculate_max_depth(self):
        max_depth = 0
        classes = self.db.collection("classes")
        for c in tqdm(classes.all().batch(), total=classes.count(), desc="Documents"):
            _id = c["_id"]
            depth = len(self.get_superclasses(c["_id"]))
            c["depth"] = depth
            classes.update(c)

            if depth > max_depth:
                max_depth = depth
        return max_depth

    def get_depth(self, doc_id):
        if "classes" not in doc_id:
            doc_id = self.get_document_from_uri(doc_id)
        if doc_id is not None:
            return self._ontology[doc_id]["depth"]

    def get_max_depth(self):
        return self.properties()["max_depth"]

    def get_max_idf(self):
        return self.properties()["max_idf"]

    def shortest_path_length(self, nodes):
        combination = tuple(sorted(nodes))
        if combination not in settings.DISTANCES:
            cursor = self.db.aql.execute(
                f"FOR v IN ANY SHORTEST_PATH @source TO @target objectproperties,classmappings,subclassof RETURN v",
                bind_vars={"source": nodes[0], "target": nodes[1]})
            length = len([x for x in cursor])
            settings.DISTANCES[combination] = length
            return combination, length
        else:
            return combination, settings.DISTANCES[combination]

    def shortest_path(self, nodes):
        pregel = self.db.pregel
        job_id = self.db.pregel.create_job(
            graph="schema-graph",
            algorithm="sssp",
            store=False,
            max_gss=100,
            thread_count=7,
            async_mode=False,
            result_field="uri",
            algorithm_params={"source": nodes[0], "target": nodes[1]}
        )
        job = pregel.job(job_id)
        print(job)
        while job["state"] == "running":
            time.sleep(1)
            job = pregel.job(job_id)
            print(job)
        print(job)
        cursor = self.db.aql.execute(
            f"FOR doc IN PREGEL_RESULT({job}) RETURN doc"
        )
        for x in cursor:
            print(x)
        pregel.delete_job(job_id)

    def _open(self):
        self.client = ArangoClient(hosts="http://" + self.host + ":" + self.port)
        self.sys_db = self.client.db()

        if self.sys_db.has_database(self.name):
            print(f"{self.name} exists. Loading...")
            self.db = self.client.db(self.name)
        else:
            print(f"{self.name} doesn't exist.")

        if self.db.has_graph("schema-graph"):
            # print(f"Graph exists. Loading...")
            self.graph = self.db.graph("schema-graph")
        else:
            print(f"Graph doesn't exist.")

    def init_indices(self):
        if self.db.has_graph("schema-graph"):
            self.graph = self.db.graph("schema-graph")
        else:
            self.graph = self.db.create_graph("schema-graph")

        try:
            self.graph.delete_edge_definition("subclassof", purge=True)
            self.graph.delete_edge_definition("objectproperties", purge=True)
            self.graph.delete_edge_definition("classmappings", purge=True)
            self.graph.delete_vertex_collection("classes", purge=True)
            self.graph.delete_vertex_collection("datatypeproperties", purge=True)
        except (VertexCollectionDeleteError, EdgeDefinitionDeleteError) as e:
            print("Collections don't exist yet.")
            print(e)

        c = self.graph.create_vertex_collection("classes")
        c.add_hash_index(fields=['uri'], unique=True)

        self.graph.create_edge_definition(
            edge_collection='subclassof',
            from_vertex_collections=['classes'],
            to_vertex_collections=['classes']
        )
        self.graph.create_edge_definition(
            edge_collection='objectproperties',
            from_vertex_collections=['classes'],
            to_vertex_collections=['classes']
        )
        self.graph.create_edge_definition(
            edge_collection='classmappings',
            from_vertex_collections=['classes'],
            to_vertex_collections=['classes']
        )

        # TODO: deal with datatype and object properties mappings

    def _calculate_diameter(self):
        query = f"FOR s IN classes FOR t IN classes FILTER s._id < t._id LET p = SUM((FOR v, e IN ANY " \
            f"SHORTEST_PATH s TO t GRAPH @graphName RETURN 1)) - 1 SORT p DESC LIMIT 1 RETURN p"
        print("Calculating diameter...")
        return [x for x in self.db.aql.execute(query, bind_vars={"graphName": self.get_graph_name()})][0]
