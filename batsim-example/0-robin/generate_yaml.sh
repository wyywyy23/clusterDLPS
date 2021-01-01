#!/bin/bash

OUT_DIR=./expe-out
mkdir -p ${OUT_DIR}

robin generate ./expe.yaml \
      --output-dir=${OUT_DIR} \
      --batcmd="batsim -p ./platforms/cluster512.xml -W ./workflows/genome.dax -e ${OUT_DIR}/out" \
      --schedcmd='batsched -v easy_bf'
