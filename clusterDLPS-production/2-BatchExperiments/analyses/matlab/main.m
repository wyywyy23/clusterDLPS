close all; clear; clc;

addpath('helper');

task_execution_trace_file = '../../output/task_execution.csv';

%% Task execution
task_execution_data = readTaskExecutionTrace(task_execution_trace_file);

f1 = figure;
plotTaskExecutionCumulative(task_execution_data, f1);

f2 = figure;
plotTaskExecutionGantt(task_execution_data, f2, [1]);