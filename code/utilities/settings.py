from collections import defaultdict


def init():
    global EC
    EC = None

    global GRAPH
    GRAPH = None

    global CONFIG
    CONFIG = None

    global FREQ
    FREQ = 0

    global DIAMETER
    DIAMETER = 0

    global DISTANCES
    DISTANCES = defaultdict(int)

    global PACT
    PACT = False

    global REDIRECT
    REDIRECT = {}

    global DISAM
    DISAM = {}

    global CAT_IDF
    CAT_IDF = None

    global TYPES_IDF
    TYPES_IDF = None

