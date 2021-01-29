#!/bin/bash

START_OFFSET=48
DURATION=12

WORKFLOW_DIR=./output/workflows_more_dags
END=`expr ${DURATION} - 1`

for (( START_HOUR=0; START_HOUR<=${END}; START_HOUR++ ))
do	
    END_HOUR=`expr ${START_HOUR} + 1`
    mkdir -p ${WORKFLOW_DIR}/${START_HOUR}-${END_HOUR}/

#    for FILE in ${WORKFLOW_DIR}/*.json; do
#	python workflow-schema/workflowhub-validator.py -s workflow-schema/workflowhub-schema.json -d ${FILE}
#    done
done

./trace2workflows ${START_OFFSET} ${DURATION} 1 1000

# ./trace2swf
