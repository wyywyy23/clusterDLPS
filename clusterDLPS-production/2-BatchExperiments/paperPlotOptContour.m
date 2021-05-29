close all; clear; clc;

figure('unit', 'inches', 'position', [0, 0, 1.17, 2]);
hold on;
box on;
grid on; grid minor;
hold on;

load workspace/data_size_opt.mat

x = results.XTrace.param_1;
y = results.XTrace.param_2;
z = results.ObjectiveTrace;

[xi, yi] = meshgrid(...
    linspace(min(x),max(x)),...
    linspace(min(y),max(y)));
zi = griddata(x,y,z, xi,yi);
contour(xi,yi,zi)
