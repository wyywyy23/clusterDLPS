close all; clear; clc;
rng default;
addpath('analyses/matlab/');

hour = 0;
n = 16;

lb = [0,   0,    0,    100];
ub = [1000, 1000, 1000, 1000];

fun = @(x)data_size_opt_func(x, hour, n);

param_1 = optimizableVariable('param_1', [lb(1), ub(1)], 'Type', 'integer');
param_2 = optimizableVariable('param_2', [lb(2), ub(2)], 'Type', 'integer');
param_3 = optimizableVariable('param_3', [lb(3), ub(3)], 'Type', 'integer');
param_4 = optimizableVariable('param_4', [lb(4), ub(4)], 'Type', 'integer');

results = bayesopt(fun, [param_1, param_2, param_3, param_4],...
                        'IsObjectiveDeterministic', true,...
                        'MaxObjectiveEvaluations', inf,...
                        'MaxTime', 3600 * 8,...
                        'AcquisitionFunctionName', 'expected-improvement-per-second-plus',...
                        'ExplorationRatio', 5);