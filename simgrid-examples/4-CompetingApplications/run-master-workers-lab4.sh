#!/bin/bash

./master-workers-lab4 small_platform.xml master-workers-lab4_d.xml --cfg=tracing:yes --cfg=tracing/actor:yes

Rscript draw_gantt.R simgrid.trace

evince Rplots.pdf &

