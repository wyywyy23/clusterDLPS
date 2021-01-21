#!/bin/bash

./wyy_simulator platforms/cluster_4096_machines_FAT_TREE.xml ../instance_trace_to_workflows/original/output/swf/container_trace.swf ../instance_trace_to_workflows/original/output/workflows/0-1/ 16 static --activate-dlps --log=wyy_simulator.th:debug --log=dlps.th:info --log=dlps.fmt:%m --log=dlps.app:file:output/dlps_events.csv --cfg=contexts/guard-size:0

