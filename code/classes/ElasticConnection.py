from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, scan


def normalise_score(results):
    new_results = []
    if results:
        scores = tuple(r["_score"] for r in results)
        min_score = min(scores)
        diff = (max(scores) - min_score)
        for r in results:
            if diff > 0:
                r["_norm_score"] = (r["_score"] - min_score) / diff
            else:
                r["_norm_score"] = 1.0
            new_results.append(r)
    return new_results


class ElasticConnection:
    def __init__(self, host, port):
        self.es = Elasticsearch([{"host": host, "port": port}])
        self.total = 0

    def add_index(self, index_name, collection, body, overwrite=False):
        """
        :param index_name: name of existing of new index to add
        :param collection: list of documents in dictionary format, e.g. [{field1: value1} {field1: value2}]
        :param overwrite: True to overwrite an existing index with index_name, False to append to index_name
        :return:
        """
        self.es.indices.put_template(name="default", body={
            "index_patterns": ["*"],
            "mappings": {
                "_source": {"enabled": True}
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
        }, create=False)
        index_name = index_name.lower()
        if overwrite and self.es.indices.exists(index=index_name):
            self.es.indices.delete(index=index_name)
            self.es.indices.create(index_name, body=body)
        for ok, result in parallel_bulk(self.es,
                                        collection,
                                        thread_count=7,
                                        index=index_name,
                                        request_timeout=30,
                                        chunk_size=500
                                        ):
            if not ok:
                print(f'Failed to load document {result.popitem()["_id"]}')

    def delete_index(self, index_name):
        self.es.indices.delete(index_name)

    def index_exists(self, index_name):
        return self.es.indices.exists([index_name])

    def search_phrase(self, keywords, related, index="", result_size=10):
        body = {
            "min_score": 1.0,
            "sort": ["_score"],
            "query": {
                "bool": {
                    "must": [{
                        "match": {
                            "labels": {
                                "query": keywords
                            }
                        }
                    },
                        {
                            "terms": {
                                "uri.keyword": related,
                                "boost": 5.0
                            }
                        }]

                }}}
        return self.es.search(index=index, body=body, allow_partial_search_results=True, size=result_size
                              )["hits"]["hits"]

    def search_combined(self, keywords, related, index, size):
        body = {
            "min_score": 1.0,
            "sort": ["_score"],
            "query": {
                "dis_max": {
                    "queries": [
                        {"term": {"labels.keyword": {"value": keywords, "boost": 5}}},
                        {"match": {"labels": keywords}},
                        {"terms": {"uri.keyword": related}},
                    ],
                    "tie_breaker": 0.2
                }
            }
        }
        return normalise_score(self.es.search(index=index, body=body, size=size)["hits"]["hits"])

    def search_combined2(self, keywords, categories, types, index, size):
        body = {
            "min_score": 1.0,
            "sort": ["_score"],
            "query": {
                "bool": {
                    "must": [
                        {"match": {"labels": keywords}},
                        {"terms": {"types.keyword": types}},
                    ],
                    "should": {"terms": {"category.keyword": categories}}
                }
            }
        }
        return normalise_score(self.es.search(index=index, body=body, size=size)["hits"]["hits"])


    def total_number_docs(self, index_name):
        res = self.es.indices.stats(index_name)
        return int(res["_all"]["primaries"]["docs"]["count"])
