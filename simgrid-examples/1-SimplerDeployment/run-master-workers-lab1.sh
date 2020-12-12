#!/bin/bash

./master-workers-lab1 small_platform.xml master-workers-lab1_d.xml --cfg=tracing:yes --cfg=tracing/actor:yes

Rscript draw_gantt.R simgrid.trace

evince Rplots.pdf &

