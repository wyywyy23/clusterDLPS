#!/bin/bash

./master-workers-lab3 small_platform.xml master-workers-lab3_d.xml --cfg=tracing:yes --cfg=tracing/actor:yes

Rscript draw_gantt.R simgrid.trace

evince Rplots.pdf &

