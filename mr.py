#!/usr/bin/env python
# encoding: utf-8

# by Liber 118
# http://liber118.com/
# licensed under a Creative Commons Attribution-ShareAlike 3.0 Unported License
# http://creativecommons.org/licenses/by-sa/3.0/


import sys


def emit (h):
    """
    emit a tuple as TSV
    """

    print "\t".join(map(lambda x: str(x), h))


def mapper (load_func, has_header=True):
    """
    map input from stdin
    """

    count = 0

    for line in sys.stdin:
        line = line.strip()

        try:
            if has_header and (count == 0):
                pass
            else:
                for t in load_func(line):
                    emit(t)

            count += 1

        except ValueError, err:
            sys.stderr.write("mapper ValueError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})
            raise


def identity_load_func (l):
    """
    identity mapper
    """

    yield l


def reducer (emit_func, has_header=True):
    """
    reduce input from mapper/shuffle
    """

    prev_key = None
    collected = []
    count = 0

    for line in sys.stdin:
        line = line.strip()

        try:
            if has_header and (count == 0):
                pass
            else:
                l = line.split("\t")
                key = l[0]
                val = l[1:]

                if prev_key and (key != prev_key):
                    if len(collected) < 1:
                        raise Exception, 'key %s has no values' % prev_key
                    else:
                        emit_func(prev_key, collected)
                        collected = []

                collected.append(val)
                prev_key = key

            count += 1

        except ValueError, err:
            sys.stderr.write("reducer ValueError: %(err)s\n%(data)s\n" % {"err": str(err), "data": line})
            raise

    emit_func(prev_key, collected)
