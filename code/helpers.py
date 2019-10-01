from collections import Counter, defaultdict
from utilities import settings


def match_column(elements, index_name, file_objects):
    type_data = []
    category_data = []
    overall_related = {}
    categories = defaultdict(list)
    types = defaultdict(list)
    elements = tuple(elements)
    for e in elements:
        row = e[0]
        elements_len = len(e[1])
        if file_objects:
            objects = {x[1] for x in file_objects}
        else:
            objects = set()
        for i in range(elements_len):
            column = e[1].index.values[i]
            keyword = e[1].iloc[i]
            if keyword != "nan" and keyword is not None:
                results = settings.EC.search_combined(keyword, tuple(objects), index=index_name, size=25)
                if results and results is not None:
                    for r in results:
                        if r["_source"]["uri"] not in overall_related:
                            overall_related[r["_source"]["uri"]] = r["_source"]["objects"]
                        objects.update({z for x in r["_source"]["objects"] for z in x[1]})
                        if r["_source"]["categories"]:
                            categories[column].extend(r["_source"]["categories"])
                        if r["_source"]["types"]:
                            types[column].extend(r["_source"]["types"])
                        for cat in r["_source"]["categories"]:
                            category_data.append((column, row, keyword, r["_source"]["labels"], r["_source"]["uri"],
                                                  cat, settings.CAT_IDF[cat], r["_norm_score"]))

                        for typ in r["_source"]["types"]:
                            if "changesets" not in typ and typ != "http://dbpedia.org/ontology/Location":
                                type_data.append((column, row, keyword, r["_source"]["labels"], r["_source"]["uri"],
                                                  typ, settings.TYPES_IDF[typ], r["_norm_score"],
                                                  (settings.GRAPH.get_depths(typ) if typ in settings.GRAPH.depths()
                                                   else 0.0)))
    if categories:
        categories_final = {k: get_most_common(v) for k, v in categories.items()}
    else:
        categories_final = {}
    if types:
        types_final = {k: get_most_common(v, True) for k, v in types.items()}
    else:
        types_final = {}
    for e in elements:
        row = e[0]
        elements_len = len(e[1])
        for i in range(elements_len):
            column = e[1].index.values[i]
            keyword = e[1].iloc[i]
            if keyword != "nan" and keyword is not None:
                if column in categories_final and column in types_final:
                    results = settings.EC.search_combined2(keyword, categories_final[column], types_final[column],
                                                           index=index_name, size=5)
                elif column in types_final and column not in categories_final:
                    results = settings.EC.search_combined2(keyword, [], types_final[column], index=index_name, size=5)
                else:
                    continue
                if results and results is not None:
                    for r in results:
                        if r["_source"]["uri"] not in overall_related:
                            overall_related[r["_source"]["uri"]] = r["_source"]["objects"]
                        for cat in r["_source"]["categories"]:
                            category_data.append((column, row, keyword, r["_source"]["labels"], r["_source"]["uri"],
                                                  cat, settings.CAT_IDF[cat], r["_norm_score"]))

                        for typ in r["_source"]["types"]:
                            if "changesets" not in typ and typ != "http://dbpedia.org/ontology/Location":
                                type_data.append((column, row, keyword, r["_source"]["labels"], r["_source"]["uri"],
                                                  typ, settings.TYPES_IDF[typ], r["_norm_score"],
                                                  (settings.GRAPH.get_depths(typ) if typ in settings.GRAPH.depths()
                                                   else 0.0)))

    return category_data, type_data, overall_related


def get_most_common(l, types_parse=False):
    counts = Counter(l)
    if types_parse:
        counts = {k: (v * (settings.GRAPH.get_depths(k) / settings.GRAPH.get_max_depth()) *
                      settings.TYPES_IDF[k] if
                      (k in settings.GRAPH.depths() and k in settings.TYPES_IDF) else 0.0)
                  for k, v in counts.items()}
    else:
        max_c = max(counts.values())
        counts = {k: ((v / max_c) * settings.CAT_IDF[k] if k in settings.CAT_IDF else 0.0) for k, v in counts.items()}
    sorted_counts = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    return tuple(x[0] for x in sorted_counts[:3])


def handle_results(row, column, keyword, results, category_data, type_data, related, r=False):
    if results and results is not None:
        for r in results:
            related.extend(r["_source"]["objects"])
            for cat in r["_source"]["categories"]:
                category_data.append(
                    (row, column, keyword, r["_source"]["labels"], r["_source"]["uri"], cat, r["_score"], r))
            for typ in r["_source"]["types"]:
                type_data.append(
                    (row, column, keyword, r["_source"]["labels"], r["_source"]["uri"], typ, r["_score"], r))
    return related, category_data, type_data
