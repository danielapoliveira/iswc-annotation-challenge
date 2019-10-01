from owlready2 import get_ontology, comment


class OWLLoader:
    def __init__(self, ontology_file):
        self.ontology_file = ontology_file
        self.ontology = self._load()
        self.annotations = [x for x in self.ontology.annotation_properties()]

    def get_classes(self):
        for c in self.ontology.classes():
            yield c

    def get_iri(self, oclass):
        return oclass.iri

    def get_label(self, oclass):
        label = oclass.label.en
        if not label:
            return oclass.label
        else:
            return label

    def get_definition(self, oclass):
        definitions = []
        for annot in (x for x in self.annotations if x.iri and "definition" in str(x.iri).lower()):
            definitions.extend(annot[oclass])

        if not definitions:
            c = comment[oclass]
            if c.en:
                return c.en
            else:
                return c
        return definitions

    def subclasses(self, oclass):
        for sc in oclass.subclasses():
            yield sc

    def get_object_properties(self):
        for op in self.ontology.object_properties():
            yield op

    def get_datatype_properties(self):
        for dp in self.ontology.data_properties():
            yield dp

    def get_annotation_properties(self):
        for ap in self.ontology.annotation_properties():
            yield ap

    def get_domain(self, prop):
        return prop.domain

    def get_range(self, prop):
        return prop.range

    def get_ontology_iri(self):
        return self.ontology.base_iri

    def _load(self):
        return get_ontology(self.ontology_file).load()
