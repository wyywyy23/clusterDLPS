#!/bin/bash

./wyy_simulator --activate-dlps --log=wyy_simulator.th:debug --log=wyy_wms.th:info --log=wyy_wms.fmt:%m --log=wyy_wms.app:file:output/task_execution.csv --log=dlps.th:info --log=dlps.fmt:%m --log=dlps.app:file:output/dlps_events.csv --cfg=contexts/guard-size:0 platforms/cluster_4096_machines_FAT_TREE.xml ../instance_trace_to_workflows/original/output/swf/container_trace.swf ../instance_trace_to_workflows/original/output/workflows/0-1/ 4 static

./wyy_simulator --activate-dlps --log=wyy_simulator.th:debug --log=wyy_wms.th:info --log=wyy_wms.fmt:%m --log=wyy_wms.app:file:output/task_execution.csv --log=dlps.th:info --log=dlps.fmt:%m --log=dlps.app:file:output/dlps_events.csv --cfg=contexts/guard-size:0 platforms/cluster_4_machines_FAT_TREE.xml ../instance_trace_to_workflows/original/output/swf/container_trace.swf ../instance_trace_to_workflows/spar_sampled/workflows/4_machines/ 4096 static
