close all; clear; clc;

addpath('helper');

task_execution_trace_file = '../../output/task_execution.csv';

%% Task execution
task_execution_data = readTaskExecutionTrace(task_execution_trace_file);

f1 = figure;
[f1, r1, d1, r2, d2] = plotTaskExecutionCumulative(task_execution_data, f1);

% f2 = figure;
% plotTaskExecutionGantt(task_execution_data, f2, []);

% f3 = figure;
% plotStartTimeDifference(task_execution_data, f3);

% p = p * 100000000000 / 8000 / 1000000;