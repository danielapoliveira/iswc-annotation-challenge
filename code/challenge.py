import argparse
import os
import shutil
from multiprocessing.pool import Pool

import numpy as np
import pandas as pd
from inpout import inpout
from tqdm import tqdm

from classes.ArangoSchemaGraph import ArangoSchemaGraph
from classes.CSVDataSource import CSVDataSource
from classes.ElasticConnection import ElasticConnection
from helpers import match_column
from utilities import settings
from utilities.Configuration import Configuration
from utilities.general import levenshtein_similarity, clean_string

TASK = ""
CELLS = None


def main(args):
    config = Configuration(args.config_file)
    arango = ArangoSchemaGraph(config)
    arango.init_graph()
    ec = ElasticConnection(config.ehost, config.eport)
    settings.init()
    settings.EC = ec
    settings.GRAPH = arango
    compute_task(config, config.task)


def compute_task(config, task):
    global CELLS
    global TASK
    TASK = task
    settings.GRAPH.uris()
    settings.GRAPH.hierarchy()
    settings.GRAPH.depths()
    settings.CAT_IDF = dict(inpout.load_iter(config.base_dir + "dbpedia-example/categories_idf.mp.lz4"))
    settings.TYPES_IDF = dict(inpout.load_iter(config.base_dir + "dbpedia-example/types_idf.mp.lz4"))
    if settings.PACT:
        if not os.path.exists(config.output_dir + "short/"):
            os.makedirs(config.output_dir + "short/")
        output_dir2 = config.output_dir + "short/"
    else:
        if not os.path.exists(config.output_dir + "split/"):
            os.makedirs(config.output_dir + "split/")
        else:
            shutil.rmtree(config.output_dir + "split/")
            os.makedirs(config.output_dir + "split/")
        output_dir2 = config.output_dir + "split/"

    if task == "cea":
        columns = ["column", "row"]
        out2 = config.output_dir + "cea2_result_error.csv"
    elif task == "cta":
        columns = ["column"]
        out2 = config.output_dir + "cta2_result_error.csv"
    elif task == "cpa":
        columns = ["column", "tail_column"]
        out2 = config.output_dir + "cpa2_result_error.csv"

    CELLS = CSVDataSource(config.task_column_file).data
    CELLS.columns = ["file"] + columns
    files = CELLS["file"].unique()
    np.random.shuffle(files)
    files_open = tuple(CSVDataSource(config.task_dir + f + ".csv") for f in files)

    with open(out2, "w") as error:
        with Pool() as pool:
            results = pool.imap_unordered(process_data, files_open)
            for r in tqdm(results, desc="Files", total=len(CELLS["file"].unique())):
                candidates = r[0]
                filename = r[1]
                with open(output_dir2 + filename + ".csv", "w") as writer:
                    if candidates is not None and not candidates.empty:
                        if task == "cea":
                            save_cea(writer, candidates, filename)
                        elif task == "cta":
                            save_cta(writer, candidates, filename)
                        elif task == "cpa" and r[2] is not None and not r[2].empty:
                            df = compute_cpa(candidates, r[2], r[3])
                            if df is not None:
                                save_cpa(writer, df, filename)
                    else:
                        error.write(filename + "\n")


def save_cea(writer, candidates, filename):
    for c in candidates.itertuples():
        writer.write("\"" + filename + "\",\"" + str(c.column) + "\",\"" + str(c.row) + "\",\"" +
                     c.candidate + "\"\n")


def save_cta(writer, candidates, filename):
    candidates = candidates.sort_values(["freq2", "depth"]).groupby("column").last().reset_index()
    for c in candidates.itertuples():
        if c.type != "nantype":
            superclasses = settings.GRAPH.get_superclasses(c.type, True)
            types = c.type
            for p in superclasses:
                if p != "http://dbpedia.org/ontology/Agent":
                    types += " " + p
            writer.write("\"" + filename + "\",\"" + str(c.column) + "\",\"" + types + "\"\n")


def save_cpa(writer, candidates, filename):
    for c in candidates.itertuples():
        writer.write("\"" + filename + "\",\"" + str(c.head_row) + "\",\"" + str(c.tail_row) + "\",\"" +
                     c.candidate + "\"\n")


def compute_cpa(head, tail, related):
    matches = []
    for head_row in head.itertuples():
        for tail_row in tail[tail["row"] == head_row.row].itertuples():
            if head_row.candidate in related:
                objects = {x: k for k, v in related[head_row.candidate] for x in v}
                if tail_row.candidate in objects:
                    matches.append((head_row.column, tail_row.column, objects[tail_row.candidate]))
    if matches:
        df = pd.DataFrame(matches, columns=["head_row", "tail_row", "candidate"])
        df["count"] = df.groupby(["head_row", "tail_row"]).transform("count")
        df = df.sort_values("count").groupby(["head_row", "tail_row"]).last().reset_index()
        return df


def process_data(csv_data):
    print(csv_data.get_file_name())
    clean_filename = csv_data.get_file_name().replace("_", " ")
    idx = clean_filename.find("#")
    clean_filename = clean_filename[:idx]
    file_objects = []
    if not clean_filename.isdigit():
        search = settings.EC.search_phrase(clean_filename, [], index="dbpedia3", result_size=1)
        if search:
            file_objects = search[0]["_source"]["objects"]
    csv_data.data = csv_data.data.applymap(clean_string)
    with Timer("Total", settings.PACT) as total:
        with Timer("Match", settings.PACT) as t:
            categories, types, related = match_column(get_column(csv_data.data, CELLS, csv_data.get_file_name()), "dbpedia3", file_objects)
        df = get_most_common(
            pd.DataFrame(categories,
                         columns=["column", "row", "keyword", "label", "candidate", "category", "cat_idf", "score"]),
            pd.DataFrame(types, columns=["column", "row", "keyword", "label", "candidate", "type", "type_idf", "score", "depth"]))

        if TASK == "cpa":
            categories_tail, types_tail, related_tail = match_column(
                get_column(csv_data.data, CELLS, csv_data.get_file_name(), "tail_column"), "dbpedia2", file_objects)
            df_tail = get_most_common(
                pd.DataFrame(categories_tail,
                             columns=["column", "row", "keyword", "label", "candidate", "category", "cat_idf", "score"]),
                pd.DataFrame(types_tail,
                             columns=["column", "row", "keyword", "label", "candidate", "type", "type_idf", "score", "depth"]))

            return df, csv_data.get_file_name(), df_tail, related, related_tail
    return df, csv_data.get_file_name()


def get_most_common(df_cat, df_type):
    global TASK
    if not df_type.empty:
        df_type.drop(df_type[df_type["type"] == "http://dbpedia.org/ontology/Agent"].index, inplace=True)
        df_type = depth(df_type, settings.GRAPH.get_diameter(), settings.GRAPH, "type")
    else:
        if TASK == "cta":
            return
    if not df_cat.empty:
        if not df_type.empty:
            df = pd.merge(df_cat, df_type[["row", "column", "candidate", "type", "type_idf", "depth"]], how="left",
                          on=["row", "column", "candidate"])
            df["category"] = df["category"].fillna("nancategory")
            df["type"] = df["type"].fillna("nantype")
        else:
            df = df_cat
    else:
        df = df_type

    if df.empty:
        return
    if not df_cat.empty and not df_type.empty:
        df = df.sort_values(by="score", ascending=False).drop_duplicates(["row", "column", "candidate", "type", "category"])
    elif df_cat.empty and not df_type.empty:
        df = df.sort_values(by="score", ascending=False).drop_duplicates(
            ["row", "column", "candidate", "type"])
    else:
        df = df.sort_values(by="score", ascending=False).drop_duplicates(
            ["row", "column", "candidate", "category"])

    df["sim"] = df.loc[~df.duplicated(subset=["row", "column", "candidate"])].apply(
        lambda x: get_max_sim(x["keyword"], x["label"]), axis=1)
    df["sim"] = df.groupby(["row", "column", "candidate"])["sim"].transform(lambda v: v.ffill())

    if not df_cat.empty:
        df = count_non_duplicates(df, "category", "freq")

    if not df_type.empty:
        df = count_non_duplicates(df, "type", "freq2")

    if not df_cat.empty:
        df["freq"] = df.groupby(["row", "column", "candidate"])["freq"].transform(max)
    if not df_type.empty:
        df["freq2"] = df.groupby(["row", "column", "candidate"])["freq2"].transform(max)

    if not df_type.empty and not df_cat.empty:
        df.loc[:, "final_score"] = (df["sim"] * df["score"] * (0.5 + (df["freq"] * 0.5) + (df["freq2"] * 0.5)))
    elif df_type.empty and not df_cat.empty:
        df.loc[:, "final_score"] = df["sim"] * df["score"] * (0.5 + (df["freq"] * 0.5))
    else:
        df.loc[:, "final_score"] = df["sim"] * df["score"] * (0.5 + (df["freq2"] * 0.5))

    idx = df.groupby(["row", "column"])["final_score"].transform(max) == df['final_score']
    df = df.loc[idx]

    df = df.drop_duplicates(subset=["column", "row"]).reset_index(drop=True)

    return df


def get_max_sim(term, labels):
    max_sim = 0
    if term:
        for l in labels:
            if l:
                sim = levenshtein_similarity(term, l)
                if sim > max_sim:
                    max_sim = sim
    return max_sim


def count_non_duplicates(df, name, new_column):
    df["count"] = np.where(~df.duplicated(subset=["row", "column", name]), 1, 0)
    df[new_column] = df.groupby([name, "column"])["count"].transform(np.sum)

    df['count_max'] = df.groupby(['column'])[new_column].transform(np.max)
    if new_column == "freq":
        df[new_column] = ((df[new_column] / df["count_max"]) * 0.7) + (df["cat_idf"] * 0.3)
    else:
        df[new_column] = ((df[new_column] / df["count_max"]) * 0.7) + (df["type_idf"] * 0.15) + ((df["depth"] / settings.GRAPH.get_max_depth()) * 0.15)
    df.loc[df[name] == "nan" + name, new_column] = 0
    return df


def get_column(data, cells, filename, column="column"):
    global TASK
    columns = cells[cells["file"] == filename][column].astype(int).unique()
    if TASK == "cea":
        rows = cells[cells["file"] == filename]["row"].astype(int)
        for d in data.loc[np.unique(rows.values), [c for c in columns if c < len(data.columns)]].iterrows():
            yield d
    else:
        for d in data.loc[1:50, [c for c in columns if c < len(data.columns)]].iterrows():
            yield d


def depth(candidates, diameter, arango, column="candidate"):
    candidates["depth"] = candidates[column].apply(arango.get_depth)
    return candidates.copy()


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description='')
    PARSER.add_argument("-f", "--file", type=str, help="CSV file", default="")
    PARSER.add_argument("-c", "--config-file", type=str, help="File with all the configurations.", default="")
    PARSER.add_argument("-p", "--print", action="store_true")
    main(PARSER.parse_args())
