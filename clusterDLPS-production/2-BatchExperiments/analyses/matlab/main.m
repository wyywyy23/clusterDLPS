close all; clear; clc;

addpath('helper');

task_execution_trace_file = '../../output/task_execution_none.csv';
link_usage_trace_file = '../../output/dlps_events_none.csv';

%% Task execution
task_execution_data = readTaskExecutionTrace(task_execution_trace_file);

f1 = figure;
[f1, r1, d1, r2, d2] = plotTaskExecutionCumulative(task_execution_data, f1);

% f2 = figure;
% plotTaskExecutionGantt(task_execution_data, f2, []);

clear task_execution_data;

link_usage_data = readLinkUsageTrace(link_usage_trace_file);

f3 = figure;
[f3, interval] = plotLinkUsage(link_usage_data, f3, 64);

figure;
histogram(interval, [0:1:3600])

% f4 = figure;
% plotLinkStates(link_usage_data, f4, 64);

% clear link_usage_data;
