#!/bin/bash

# by Liber 118
# http://liber118.com/
# licensed under a Creative Commons Attribution-ShareAlike 3.0 Unported License
# http://creativecommons.org/licenses/by-sa/3.0/


export LC_ALL='C'


## prints a header (first line of input) then runs the specified
## command on the "body" (rest of the input), for use in a pipeline,
## such as MapReduce at command line

body () {
    IFS= read -r header
    printf '%s\n' "$header"
    "$@"
}


## run the MapReduce pipeline

cat example.txt | \
  ./example.py map1 en.stop | \
  body sort -k1 | \
  ./example.py red1 | \
  body sort -k2 -rn \
> wc.tsv
