import itertools

from rdflib import Graph, RDF, RDFS, OWL


class RDFLoader:
    def __init__(self, ontology_file, format):
        self.ontology_file = ontology_file
        self.format = format
        self.ontology = self._load()

    def get_classes(self):
        for c in itertools.chain(self.ontology.subjects(predicate=RDF.type, object=RDFS.Class),
                                 self.ontology.subjects(predicate=RDF.type, object=OWL.Class)):
            yield c

    def get_iri(self, oclass):
        return oclass.toPython()

    def get_label(self, oclass):
        return self.ontology.label(oclass).toPython()

    def get_comment(self, oclass):
        return self.ontology.comment(oclass).toPython()

    def subclasses(self, oclass):
        for sc in self.ontology.objects(subject=oclass, predicate=RDFS.subClassOf):
            yield sc

    def get_object_properties(self):
        for op in itertools.chain(self.ontology.subjects(predicate=RDF.type, object=OWL.ObjectProperty),
                                  self.ontology.subjects(predicate=RDF.type, object=RDF.Property)):
            tmp = self.ontology.objects(subject=op, predicate=RDFS.range)
            if tmp:
                for each in tmp:
                    if list(self.ontology.objects(each, RDF.type)):
                        yield op
            else:
                yield op

    def get_datatype_properties(self):
        for op in itertools.chain(self.ontology.subjects(predicate=RDF.type, object=OWL.DatatypeProperty),
                                  self.ontology.subjects(predicate=RDF.type, object=RDF.Property)):
            tmp = self.ontology.objects(subject=op, predicate=RDFS.range)
            if tmp:
                for each in tmp:
                    if not tuple(self.ontology.objects(each, RDF.type)):
                        yield op
            else:
                yield op

    def get_annotation_properties(self):
        for ap in self.ontology.subjects(predicate=RDF.type, object=OWL.AnnotationProperty):
            yield ap

    def get_domain(self, prop):
        return tuple(self.ontology.objects(subject=prop, predicate=RDFS.domain))

    def get_range(self, prop):
        return tuple(self.ontology.objects(subject=prop, predicate=RDFS.range))

    def get_ontology_iri(self):
        return self.ontology.identifier.n3()

    def _load(self):
        g = Graph()
        g.parse(self.ontology_file, format=self.format)
        return g
