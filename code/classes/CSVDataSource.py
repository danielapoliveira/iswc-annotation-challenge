from pandas import read_csv, np
from pandas.errors import ParserError


class CSVDataSource:
    def __init__(self, filename, sep=","):
        super().__init__()
        self.filename = filename
        self.separator = sep
        self.data = self._read()

    def get_instances(self):
        return self.data.iloc[1:, :]

    def get_instance(self, column):
        return self.data.iloc[1:, column]

    def get_header(self):
        return self.data.iloc[0]

    def get_file_path(self):
        return self.filename

    def get_file_name(self):
        return self.filename.split("/")[-1].split(".csv")[0]

    def number_columns(self):
        return len(self.data.columns)

    def _read(self):
        try:
            return read_csv(self.filename, sep=self.separator, header=None,
                             quotechar='"', skipinitialspace=True, escapechar='\\', dtype=str)
        except ParserError:
            csv = read_csv(self.filename, sep=self.separator, header=None,
                            skiprows=1, quotechar='"', skipinitialspace=True, escapechar='\\')
            # print(list(range(len(csv.columns))))
            csv.loc[-1] = list(np.nan for _ in range(len(csv.columns)))
            csv.index = csv.index + 1
            csv.sort_index(inplace=True)
            return csv

    def __iter__(self):
        for x in self.data:
            yield x

    def __str__(self):
        return self.data.to_string()
