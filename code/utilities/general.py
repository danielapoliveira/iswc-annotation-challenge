import re
import string
from base64 import b32encode

from urllib.parse import unquote

import Levenshtein
import dateutil
from ftfy import fix_encoding, fix_text
from ftfy.fixes import fix_partial_utf8_punct_in_1252, decode_escapes, remove_bom, remove_control_chars, \
    fix_latin_ligatures, uncurl_quotes
from pandas import isnull


def clean_string(s):
    s = str(s)
    if isnull(s):
        return None
    elif re.search('[a-zA-Z]', s) is None:
        return None
    else:
        s = remove_bom(s)
        s = remove_control_chars(s)
        s = fix_encoding(s)
        s = fix_text(s)
        s = fix_partial_utf8_punct_in_1252(s)
        s = decode_escapes(s)
        s = fix_latin_ligatures(s)
        s = uncurl_quotes(s)
        s = s.replace("Äu0087", "ć")
        s = s.replace("Äu0090", "Đ")
        s = s.replace("Ãu0096", "Ö")
        s = s.replace("Åu008D", "ō")

        s = s.replace("\\", " ")
        s = s.replace("/", " ")
        s = s.replace("Ã¶", "ö")

        p = re.compile("^\w+[A-Z]{1}\w*$")
        if p.search(s):
            # From: https://stackoverflow.com/a/37697078
            s = re.sub('(?!^)([A-Z][a-z]+)', r'\1', s)

        new_string = ""
        p = False
        for letter in s:
            if letter in "([":
                p = True
            elif letter in ")]":
                p = False
                continue
            if not p:
                new_string += letter
        return new_string.strip()

def remove_brackets(s):
    new = ""
    for l in s:
        if l == "(":
            return new.strip()
        else:
            new += l
    return new.strip()


def chunks(l, n):
    """Yield successive n-sized chunks from l. https://stackoverflow.com/a/312464"""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def levenshtein_similarity(s1, s2):
    s1 = s1.lower().strip().replace("–", "")
    s2 = s2.lower().strip().replace("–", "")
    if s1 and s2 and len(s1) > 1:
        new_s2 = ""
        if s1[1] == ".":
            split_s2 = s2.split(" ")
            new_s2 += split_s2[0][0] + ". " + " ".join(split_s2[1:])
            s2 = new_s2
    s1 = remove_brackets(s1).translate(str.maketrans('', '', string.punctuation))
    s2 = remove_brackets(s2).translate(str.maketrans('', '', string.punctuation))
    if s1 and s2:
        return 1 - (Levenshtein.distance(s1, s2) / max(len(s1), len(s2)))
    else:
        return 0.0


def label_from_url(url):
    return remove_brackets(unquote(url).split("/")[-1].replace("_", " "))
