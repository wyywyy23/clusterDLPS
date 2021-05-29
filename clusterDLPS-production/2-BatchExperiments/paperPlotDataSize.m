close all; clear; clc;

n = 4;
hour0 = 0;
hours = 5;
a = 209000000;
b = 0;
rhoPool = [0.2, 0.5, 1, 2, 5];

folder = 'output/filesize/';

figure('unit', 'inches', 'position', [0, 0, 3.49, 2]);
pbaspect([3,1,1]);
xlim([1e5, 1e10]);
box on;
grid on; grid minor;
hold on;

for r = 1:length(rhoPool)
    rho = rhoPool(r);
    data = [];
    [status, ~] = system(['rm ', folder, '*']);
    for hour = hour0:hour0+hours-1
        command = ['./wyy_simulator --activate-dlps ',...
            '--cfg=network/crosstraffic:0 --cfg=surf/precision:1.0e-15 ',...
            '--cfg=contexts/guard-size:0 --cfg=network/optim:Full ',...
            'platforms/cluster_', num2str(n), '_machines_FAT_TREE.xml ',...
            '../instance_trace_to_workflows/original/output/swf/container_trace.swf ',...
            '../instance_trace_to_workflows/original/output/workflows_without_file_size/',...
            num2str(hour), '-', num2str(hour+1), '/ ',...
            num2str(n), ' ',...
            num2str(rho), ' ',...
            num2str(a), ' ', num2str(b), ' ',...
            'static --cfg=network/dlps:none ',...
            '--log=wyy_wms.th:info --log=wyy_wms.fmt:%m ',...
            '--log=wyy_wms.app:file:output/task_execution.csv'];
        [status, ~] = system(command);
        
        filePattern = fullfile(folder, '*.csv');
        jobFiles = dir(filePattern);
        for i = 1:length(jobFiles)
            data = [data; csvread([folder, jobFiles(i).name])];
        end
    end
    h(r) = cdfplot(data);
    set(gca, 'xscale', 'log');
    drawnow;
end

set(findall(gca, 'Type', 'Line'),'LineWidth',1);
set(h(5), 'color', [220,5,12]/255);
set(h(4), 'color', [247,240,86]/255);
set(h(3), 'color', [78,178,101]/255);
set(h(2), 'color', [123,175,222]/255);
set(h(1), 'color', [25,101,176]/255);
xlabel('Data size (bytes)')
ylabel('Percentage (%)')
title('')
set(gca, 'fontsize', 8, 'fontname', 'Fira Sans',...
    'layer', 'top');

% legend({'\rho = 0.2', '\rho = 0.5', '\rho = 1.0', '\rho = 2.0', '\rho = 5.0'}, 'fontsize', 8, 'fontname', 'Fira Sans', 'location', 'northwest')

saveas(gcf, 'figs/datasize.fig');