close all; clear; clc;
addpath(genpath('analyses/matlab/'));
rng default;

hour = 0;
n = 8;

lb = [-250, -2500];
ub = [ 250,  2500];
intCol = [1, 2];

% options = optimoptions('ga', 'PopulationSize', 10,...
%                              'MaxStallGenerations', 10,...
%                              'Display', 'iter',...
%                              'PlotFcn', 'gaplotbestf');

fun = @(x)data_size_optimization_func(x, hour, n);

% [param, fval] = ga(fun, 2, [], [], [], [], lb, ub, [], intCol, options);

param_1 = optimizableVariable('param_1', [lb(1), ub(1)], 'Type', 'integer');
param_2 = optimizableVariable('param_2', [lb(2), ub(2)], 'Type', 'integer');

results = bayesopt(fun, [param_1, param_2], 'IsObjectiveDeterministic', true,...
                                            'MaxObjectiveEvaluations', 100,...
                                            'MaxTime', 3600 * 8,...
                                            'AcquisitionFunctionName', 'expected-improvement-plus',...
                                            'ExplorationRatio', 1);