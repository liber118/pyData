#!/usr/bin/env python
# encoding: utf-8

# by Liber 118
# http://liber118.com/
# licensed under a Creative Commons Attribution-ShareAlike 3.0 Unported License
# http://creativecommons.org/licenses/by-sa/3.0/


import mr
import sys


######################################################################
## global declarations

debug = False # True

stop_words = set([])


######################################################################
## utility functions

def read_stop_words (filename):
    """
    load a stop-words file
    """

    global stop_words

    with open(filename, "r") as f:
        for line in f:
            stop_words.add(line.strip())


######################################################################
## mappers and reducers

def load_text (text):
    global stop_words

    for ch in ",;!?(){}[]*%&^$#@<>'\"":
        text = text.replace(ch, " ")

    for token in text.split(" "):
        token = token.strip().strip(".").strip("-").strip(":").lower()

        if (len(token) > 1) and (token not in stop_words) and (not token.isdigit()):
            yield [token, 1]


def emit_word_count (key, collected):
    token = key

    try:
        mr.emit((token, len(collected)))

    except ValueError, err:
        sys.stderr.write("emit_word_count ValueError: %(err)s\n%(data)s\n" % {"err": str(err), "data": str(collected)})
        raise


if __name__ == '__main__':
    mode = sys.argv[1]

    if mode == "map1":
        read_stop_words(sys.argv[2])
        mr.emit(("token", "count"))
        mr.mapper(load_text)

    elif mode == "red1":
        mr.emit(("token", "count"))
        mr.reducer(emit_word_count)
