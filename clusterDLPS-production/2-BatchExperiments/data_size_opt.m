close all; clear; clc;
rng default;
addpath('analyses/matlab/');

hour = 4;
n = 4;

lb = [0,   400];
ub = [300, 600];

fun = @(x)data_size_opt_func(x, hour, n);

param_1 = optimizableVariable('param_1', [lb(1), ub(1)], 'Type', 'integer');
param_2 = optimizableVariable('param_2', [lb(2), ub(2)], 'Type', 'integer');


results = bayesopt(fun, [param_1, param_2],...
                        'IsObjectiveDeterministic', true,...
                        'MaxObjectiveEvaluations', 100,...
                        'MaxTime', 3600 * 8,...
                        'AcquisitionFunctionName', 'expected-improvement-per-second-plus',...
                        'ExplorationRatio', 0.5);