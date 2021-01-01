#!/bin/bash

for nMachines in 4 8 16 32 64 128 256 512 1024 2048 4096
do	
    WORKFLOW_DIR="./workflows/${nMachines}_machines"
    mkdir -p ${WORKFLOW_DIR}
    ./trace2workflows ${nMachines}

    for FILE in ${WORKFLOW_DIR}/*.json; do
	python workflow-schema/workflowhub-validator.py -s workflow-schema/workflowhub-schema.json -d ${FILE}
    done
done	
