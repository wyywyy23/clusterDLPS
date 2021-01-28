close all; clear; clc;

addpath('helper');

task_execution_trace_file = '../../output/task_execution.csv';
link_usage_trace_file = '../../output/dlps_events.csv';

%% Task execution
% task_execution_data = readTaskExecutionTrace(task_execution_trace_file);

% f1 = figure;
% [f1, r1, d1, r2, d2] = plotTaskExecutionCumulative(task_execution_data, f1);

% f2 = figure;
% plotTaskExecutionGantt(task_execution_data, f2, [3]);

% clear task_execution_data;

link_usage_data = readLinkUsageTrace(link_usage_trace_file);

f3 = figure;
plotLinkUsage(link_usage_data, f3, 64);

clear link_usage_data;
