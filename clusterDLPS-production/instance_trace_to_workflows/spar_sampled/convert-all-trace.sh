#!/bin/bash

for nMachines in 4 8 16 32 64 4096
do	
    WORKFLOW_DIR="./workflows/${nMachines}_machines"
    mkdir -p ${WORKFLOW_DIR}
    ./trace2workflows ${nMachines} 0.015

#    for FILE in ${WORKFLOW_DIR}/*.json; do
#	python workflow-schema/workflowhub-validator.py -s workflow-schema/workflowhub-schema.json -d ${FILE}
#    done
done	
