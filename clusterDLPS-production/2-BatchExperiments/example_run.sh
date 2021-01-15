#!/bin/bash

./wyy_simulator platforms/cluster_4_machines_FAT_TREE.xml background_trace/NASA-iPSC-1993-3.swf ../instance_trace_to_workflows/alibaba/workflows/4_machines/ fcfs --activate-dlps --log=wyy_simulator.th:debug --log=dlps.th:info --log=dlps.fmt:%m --log=dlps.app:file:output/dlps_events.csv --cfg=contexts/guard-size:0

