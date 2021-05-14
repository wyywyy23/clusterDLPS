#!/bin/bash

for mode in none on-off laser full

do
# ./wyy_simulator --activate-dlps --cfg=network/crosstraffic:0 --cfg=surf/precision:1.0e-15 --log=wyy_simulator.th:info --log=wyy_wms.th:info --log=wyy_wms.fmt:%m --log=wyy_wms.app:file:output/task_execution_${mode}.csv --log=dlps.th:info --log=dlps.fmt:%m --log=dlps.app:file:output/dlps_events_${mode}.csv --cfg=contexts/guard-size:0 platforms/cluster_64_machines_FAT_TREE.xml ../instance_trace_to_workflows/original/output/swf/container_trace.swf ../instance_trace_to_workflows/spar_sampled/workflows/64_machines/ 4096 0.5 static --cfg=network/dlps:${mode}

./wyy_simulator --activate-dlps --cfg=network/crosstraffic:0 --cfg=surf/precision:1.0e-15 --log=wyy_simulator.th:info --cfg=contexts/guard-size:0 --cfg=network/optim:Lazy platforms/cluster_16_machines_FAT_TREE.xml ../instance_trace_to_workflows/original/output/swf/container_trace.swf ../instance_trace_to_workflows/original/output/workflows_without_file_size/0-1/ 16 0.5 178000000 346000000 18000000 239000000 static --cfg=network/dlps:${mode} --log=wyy_wms.th:info --log=wyy_wms.fmt:%m --log=wyy_wms.app:file:output/task_execution_${mode}.csv --log=dlps.th:info --log=dlps.fmt:%m --log=dlps.app:file:output/dlps_events_${mode}.csv
done
