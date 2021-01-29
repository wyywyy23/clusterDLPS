#!/bin/bash

for nMachines in 4 8 16 32 64
do	
    WORKFLOW_DIR="./workflows_more_dags/${nMachines}_machines"
    mkdir -p ${WORKFLOW_DIR}
    ./trace2workflows ${nMachines} 1

#    for FILE in ${WORKFLOW_DIR}/*.json; do
#	python workflow-schema/workflowhub-validator.py -s workflow-schema/workflowhub-schema.json -d ${FILE}
#    done
done	
