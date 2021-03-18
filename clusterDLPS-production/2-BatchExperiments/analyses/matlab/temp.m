close all; clear; clc;

addpath('helper');

task_execution_trace_file = '../../output/task_execution_none.csv';
task_execution_data = readTaskExecutionTrace(task_execution_trace_file);
exe_time_none = task_execution_data{11} - task_execution_data{6};
[~, I] = sort(task_execution_data{2});
exe_time_none = exe_time_none(I);

task_execution_trace_file = '../../output/task_execution_on-off.csv';
task_execution_data = readTaskExecutionTrace(task_execution_trace_file);
exe_time_on_off = task_execution_data{11} - task_execution_data{6};
[~, I] = sort(task_execution_data{2});
exe_time_on_off = exe_time_on_off(I);

task_execution_trace_file = '../../output/task_execution_laser.csv';
task_execution_data = readTaskExecutionTrace(task_execution_trace_file);
exe_time_laser = task_execution_data{11} - task_execution_data{6};
[~, I] = sort(task_execution_data{2});
exe_time_laser = exe_time_laser(I);

task_execution_trace_file = '../../output/task_execution_full.csv';
task_execution_data = readTaskExecutionTrace(task_execution_trace_file);
exe_time_full = task_execution_data{11} - task_execution_data{6};
[~, I] = sort(task_execution_data{2});
exe_time_full = exe_time_full(I);

overhead_on_off = mean((exe_time_on_off(exe_time_none ~= 0) - exe_time_none(exe_time_none ~= 0)) ./ exe_time_none(exe_time_none ~= 0), 'omitnan');
overhead_laser = mean((exe_time_laser(exe_time_none ~= 0) - exe_time_none(exe_time_none ~= 0)) ./ exe_time_none(exe_time_none ~= 0), 'omitnan');
overhead_full = mean((exe_time_full(exe_time_none ~= 0) - exe_time_none(exe_time_none ~= 0)) ./ exe_time_none(exe_time_none ~= 0), 'omitnan');

figure;
hold on;
scatter(0, 1, 'o');
scatter(overhead_on_off, 1, 'x');
scatter(overhead_laser, 1, '^');
scatter(overhead_full, 1, 's');
set(gca, 'yscale', 'log');
% set(gca, 'xscale', 'log');

