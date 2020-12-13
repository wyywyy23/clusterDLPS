#!/bin/bash

./master-workers-lab2 small_platform.xml master-workers-lab2_d.xml --cfg=tracing:yes --cfg=tracing/actor:yes

Rscript draw_gantt.R simgrid.trace

evince Rplots.pdf &

