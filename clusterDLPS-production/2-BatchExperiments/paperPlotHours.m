close all; clear; clc;

addpath('matlab_helper');

%% Config
n = 4;
a = 209000000;
b = 0;
rho = 0.5;

corner = 'SS';
[delta1, delta2, delta3] = corner2delay(corner);

strategyPool = {'none', '4-bit'};
modePool     = {'none', 'full'};
bitPool      = [0, 4];

folder = 'output/';
subfolder_task = 'task_trace/';
subfolder_comm = 'comm_trace/';

%% Rerun experiments
for hour = 22
    
    [~, ~] = system(['rm ', folder, '*.csv']);
    [~, ~] = system(['rm ', folder, subfolder_task, '*/*.trace']);
    [~, ~] = system(['rm ', folder, subfolder_comm, '*/*.trace']);
    
    for run = 1:length(strategyPool)
        strategy = strategyPool{run};
        mode = modePool{run};
        bit = bitPool(run);
        
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
            'static --cfg=network/dlps:', mode, ' ',...
            '--cfg=network/dlps/idle-predictor-bits:', num2str(bit), ' ',...
            '--cfg=network/dlps/delay/laser-waking:', num2str(delta1), ' ',...
            '--cfg=network/dlps/delay/laser-stabilizing:', num2str(delta2), ' ',...
            '--cfg=network/dlps/delay/tuning:', num2str(delta3), ' ',...
            '--log=wyy_wms.th:info --log=wyy_wms.fmt:%m ',...
            '--log=wyy_wms.app:file:output/task_execution_', strategy, '.csv ',...
            '--log=dlps.th:info --log=dlps.fmt:%m ',...
            '--log=dlps.app:file:output/dlps_events_', strategy, '.csv'];
        
        [status, ~] = system(command);
        
       
        
    end
    [energyNone, bitsNone] = getNetworkEnergyFromTrace('none', 0, 0);
    [energy4Bit, bits4Bit] = getNetworkEnergyFromTrace('4-bit', 1, 1);
    saving(hour + 1) = (energyNone/bitsNone*1e12 - energy4Bit/bits4Bit*1e12)/(energyNone/bitsNone*1e12) * 48.674591433550320/1.090936803576072e+06;
end

