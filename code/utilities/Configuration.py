import configparser


class Configuration:
    def __init__(self, file):
        self.config = configparser.ConfigParser()
        self.config.read(file)

        self.base_dir = self._add_option("DEFAULT", "homedir", True)

        self.task_dir = self._add_option("DEFAULT", "taskdir", True)
        self.task_column_file = self._add_option("DEFAULT", "taskcolumnfile")
        self.task = self._add_option("DEFAULT", "task")
        self.output_dir = self._add_option("DEFAULT", "outdir", True)
        self.update_file = self._add_option("DEFAULT", "updatefile", False)

        self.data = self._add_option("DEFAULT", "data")
        self.map_file = self._add_option("DEFAULT", "mappings")
        self.ontology_dir = self._add_option("DEFAULT", "ontologydir", True)

        self.aport = self._add_option("Arangodb", "port")
        self.ahost = self._add_option("Arangodb", "host")
        self.adb = self._add_option("Arangodb", "database")

        self.ehost = self._add_option("Elasticsearch", "host")
        self.eport = self._add_option("Elasticsearch", "port")
        self.prefix = self._add_option("DEFAULT", "prefix")

    def _add_option(self, field1, field2, directory=False):
        if self.config.has_option(field1, field2):
            if directory:
                return self.config.get(field1, field2) + ('/' if self.config.get(field1, field2)[-1:] != '/' else "")
            else:
                return self.config.get(field1, field2)
        return ""
