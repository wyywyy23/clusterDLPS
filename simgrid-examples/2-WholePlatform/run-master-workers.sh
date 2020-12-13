#!/bin/bash

./master-workers small_platform.xml master-workers_d.xml --cfg=tracing:yes --cfg=tracing/actor:yes

Rscript draw_gantt.R simgrid.trace

evince Rplots.pdf &
