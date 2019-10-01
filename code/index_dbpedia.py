import argparse
import math
from collections import defaultdict, Counter
from datetime import datetime
from itertools import chain
from urllib.parse import unquote

from inpout import inpout
from tqdm import tqdm

from classes.ElasticConnection import ElasticConnection
from utilities.Configuration import Configuration
from utilities.general import label_from_url


def pack_update(path, filename):
    types = {}
    labels = {}
    with open(filename, "rb") as f, open(path + "update_error_new.log", "wb") as error:
        for line in tqdm(f, total=516759253, desc="Lines"):
            try:
                decoded_line = line.decode("utf8")
            except UnicodeDecodeError:
                error.write(line)
                continue
            split_items = decoded_line.split("./downloads.dbpedia.org/live/changesets/")
            if len(split_items) == 1:
                split_items = decoded_line.split("dbpedia.org/live/changesets/")
            if len(split_items) == 1:
                split_items = decoded_line.split("live/changesets/")
            if len(split_items) == 1:
                split_items = decoded_line.split("changesets/")
            if len(split_items) == 1:
                split_items = decoded_line.split("downloads.dbpedia.org/live")
            if len(split_items) == 1:
                print(line)
                print(split_items)

            for each in (s for s in split_items if s):
                split1 = each.split(".nt.gz:")
                filename = split1[0]
                try:
                    number_code = [int(d) for d in filename.split(".")[3].split("/")[3:]]
                    date = datetime(number_code[0], number_code[1], number_code[2], number_code[3])
                    k = (date, number_code[4])
                    data = split1[1].replace("<", "").replace(" .\n", "").replace("@en", "")
                    triple = data.split("> ")
                    obj = (triple[0].replace(">", ""), triple[2].replace(">", ""))
                    # if len(split_items) > 1:
                    #     print(decoded_line)
                    #     print(split_items)
                    #     print(k, obj)
                except (IndexError, ValueError) as e:
                    error.write(line)
                    continue
                if "label" in data:
                    d_out = labels
                else:
                    d_out = types
                if k not in d_out:
                    d_out[k] = {}
                if "added" in filename:
                    key = "add"
                elif "reinserted" in filename:
                    key = "reinsert"
                else:
                    key = "remove"

                if key not in d_out[k]:
                    d_out[k][key] = []
                d_out[k][key].append(obj)

    print(f"Packing types")
    with inpout.data_pack(path + "processed_updates/types_new.mp.lz4") as pack:
        for t in sorted(types.items(), key=lambda kv: (kv[0][0], kv[0][1])):
            pack(t)
    print(f"Packing labels")
    with inpout.data_pack(path + "processed_updates/labels_new.mp.lz4") as pack:
        for t in sorted(labels.items(), key=lambda kv: (kv[0][0], kv[0][1])):
            pack(t)


def update_files(triples, update_file, output, redirects, labels=False):
    with inpout.data_unpacker(update_file, use_list=False) as unpacker:
        for date, obj in tqdm(unpacker, desc="Updating file"):
            for action, changes in obj.items():
                for original, change in changes:
                    stop = False
                    if "http://dbpedia.org/resource/" not in original:
                        stop = True

                    if not stop and action != "remove":
                        change = change.replace("@en", "").replace(".\n", "").replace("\"", "")
                        if "http" not in change:
                            if original not in triples:
                                triples[original] = set()

                            triples[original].add(change)

    with open(output, "w") as out:
        for uri, items in tqdm(triples.items(), desc="Writing"):
            for each in items:
                if not labels:
                    out.write("<" + uri + "> " + "<rdf:type>" + " <" + each + "> . \n")
                else:
                    out.write("<" + uri + "> " + "<label>" + " \"" + each + "\" . \n")


def parse_triple(line):
    return tuple(t.replace("<", "").replace("\"", "").replace("@en", "").replace(" .", "")
                 for t in line.strip().split("> "))


def read_triples(filename, name="Triples"):
    triples = defaultdict(set)
    with open(filename) as f:
        for line in tqdm(f, total=sum(1 for _ in open(filename)), desc=name):
            if line.startswith("<"):
                triple = parse_triple(line)
                if triple[2] not in triples[triple[0]]:
                    triples[triple[0]].add(triple[2])
    return triples


def read_spo(filename, name="spo"):
    triples = defaultdict(lambda: defaultdict(set))
    with open(filename) as f:
        for line in tqdm(f, total=sum(1 for _ in open(filename)), desc=name):
            if line.startswith("<"):
                triple = parse_triple(line)
                if triple[2] not in triples[triple[0]]:
                    triples[triple[0]][triple[1]].add(triple[2])
    return triples


def parse_redirects(fredirect):
    num_lines2 = sum(1 for _ in open(fredirect))
    triples = {}
    with open(fredirect) as fr:
        for line in tqdm(fr, total=num_lines2, desc="Redirects"):
            if line.startswith("<"):
                triple = parse_triple(line)
                triples[triple[0]] = triple[2]

    return triples


def create_index(types_file, labels_file, objects_file, categories_file, disam_file, redirects, language, ec, ids=None):
    merged = defaultdict(list)
    for k, v in tqdm(inpout.load_iter(labels_file), desc="Labels"):
        uri, r = redirect(k, redirects)
        merged[uri].append(("label", v, r))
    for k, v in tqdm(inpout.load_iter(types_file), desc="Types"):
        uri, r = redirect(k, redirects)
        merged[uri].append(("types", set(t for t in v if "http://dbpedia.org/ontology/" in t and
                         "Wikidata" not in t and "purl.org" not in t), False))
    for k, v in tqdm(inpout.load_iter(objects_file), desc="Objects"):
        uri, r = redirect(k, redirects)
        merged[uri].append(("objects", v, False))
    for k, v in tqdm(inpout.load_iter(categories_file), desc="categories"):
        uri, r = redirect(k, redirects)
        merged[uri].append(("categories", set(v), False))
    for k, v in tqdm(inpout.load_iter(disam_file), desc="Disam"):
        uri, r = redirect(k, redirects)
        merged[uri].append(("disam", v, False))

    all_items = ("types", "objects", "categories", "disam")
    documents = []
    for uri, items in tqdm(merged.items(), desc="Processing"):
        document = defaultdict(set)
        document["uri"] = uri
        labels = set()
        redirected = {}
        cont_processing = True
        for name, item, rd in items:
            if name == "label":
                l = item.pop()
                if "disambiguation" in l:
                    cont_processing = False
                    break
                labels.add(l)
                if rd:
                    redirected[l] = "redirected"
                else:
                    redirected[l] = "original"
            elif name == "objects":
                # TODO: Test if it worked
                pair = tuple(map(lambda x: (x[0], tuple(x[1])), (y for y in item.items() if y[0] != "http://dbpedia.org/ontology/type")))
                document[name].update(pair)
                document["related"] = (y[0] for y in item.items() if y[0] != "http://dbpedia.org/ontology/type")
            else:
                document[name].update(item)
        if cont_processing:
            if not labels:
                label = label_from_url(uri)
                labels.add(label)
                redirected[label] = "from_uri"
            if len(document.keys()) == 1:
                continue

            for each in all_items:
                if each not in document:
                    document[each] = ()
                else:
                    document[each] = tuple(document[each])

            document["labels"] = list(labels)
            documents.append(document)

    print("Creating index...")
    index_name = "dbpedia4"
    if ec.index_exists(index_name):
         ec.delete_index(index_name)
    ec.add_index(index_name, tuple(documents), body={
        "mappings": {"properties": {
            "uri": {
                "type": "keyword"
            },
            "redirect": {
                "type": "keyword"
            },
            "labels": {
                "type": "text"
            }
        }}
    }, overwrite=False)


def redirect(uri, redirects):
    if uri in redirects:
        return redirects[uri], True
    else:
        return unquote(uri), False


def calculate_idf(d):
    chain_values = tuple(chain(*d.values()))
    set_values = set(chain_values)
    N = len(set_values)
    counter = Counter(chain_values)
    idf_dict = {}
    for v in tqdm(set_values, desc="Values"):
        idf_dict[v] = math.log10(N / counter[v])
    return idf_dict


def load_and_idf(filename, outfile):
    print(f"Loading {filename}")
    d = dict(inpout.load_iter(filename))
    idf = calculate_idf(d)
    inpout.save_iter(idf.items(), outfile)

def main(args):
    config = Configuration(args.config)
    folder = config.base_dir + ('/' if config.base_dir[-1] != '/' else "")

    if args.pack_updates:
        pack_update(folder, config.update_file)
    if args.pack_redirect:
        redirects = parse_redirects(folder + "redirects/redirects_en.ttl")
        print("Packing redirects...")
        inpout.save_iter(redirects.items(), folder + "redirects.mp.lz4")
    else:
        print("Reading redirects...")
        redirects = dict(inpout.load_iter(folder + "redirects.mp.lz4", use_list=False))

    if args.update_types:
        update_files(read_triples(folder + "types/instance_types_en.ttl"),
                     folder + "processed_updates/types.mp.lz4",
                     folder + "types/updated_types.ttl", redirects)
        type_triples = read_triples(folder + f"types/updated_types.ttl", "Types")
        print("Packing types...")
        inpout.save_iter(type_triples.items(), folder + "types/updated_types.mp.lz4")
    if args.update_labels:
        update_files(read_triples(folder + "labels/labels_en.ttl"),
                     folder + "processed_updates/labels.mp.lz4",
                     folder + "labels/updated_labels.ttl", redirects, True)
        print("Packing labels")
        label_triples = read_triples(folder + "labels/updated_labels.ttl", "Labels")
        inpout.save_iter(label_triples.items(), folder + "labels/updated_labels.mp.lz4")

    if args.pack_triples:
        object_triples = read_spo(folder + f"objects/mappingbased_objects_en.ttl", "Objects")
        print("Packing objects...")
        inpout.save_iter(object_triples.items(), folder + f"objects/mappingbased_objects_en.mp.lz4")
    if args.pack_categories:
        categories = read_triples(folder + "article_categories_en.ttl", "Categories")
        print("Packing categories...")
        inpout.save_iter(categories.items(), folder + "article_categories_en.mp.lz4")
    if args.pack_disambiguations:
        disam = read_triples(folder + "disambiguations_en.ttl", "Disambiguations")
        print("Packing categories...")
        inpout.save_iter(disam.items(), folder + "disambiguations_en.mp.lz4")

    if args.idf:
        load_and_idf(folder + "article_categories_en.mp.lz4", folder + "categories_idf.mp.lz4")
        load_and_idf(folder + f"types/updated_types.mp.lz4", folder + "types_idf.mp.lz4")

    ec = ElasticConnection(config.ehost, config.eport)

    types_file = folder + f"types/updated_types.mp.lz4"
    objects_file = folder + f"objects/mappingbased_objects_en.mp.lz4"
    categories_file = folder + "article_categories_en.mp.lz4"
    disam_file = folder + "disambiguations_en.mp.lz4"
    create_index(types_file, folder + "labels/updated_labels.mp.lz4", objects_file, categories_file,
                     disam_file, redirects, "en", ec)

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description='')
    PARSER.add_argument("-c", "--config", type=str, help="CSV file")
    PARSER.add_argument("-n", "--new_run", action="store_true")
    PARSER.add_argument("-p", "--pack_updates", action="store_true")
    PARSER.add_argument("-pr", "--pack_redirect", action="store_true")
    PARSER.add_argument("-pt", "--pack_triples", action="store_true")
    PARSER.add_argument("-pc", "--pack_categories", action="store_true")
    PARSER.add_argument("-pd", "--pack_disambiguations", action="store_true")
    PARSER.add_argument("-ut", "--update_types", action="store_true")
    PARSER.add_argument("-ul", "--update_labels", action="store_true")
    PARSER.add_argument("-idf", "--idf", action="store_true")
    main(PARSER.parse_args())
