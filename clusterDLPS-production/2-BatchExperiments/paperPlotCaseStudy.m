close all; clear; clc;

addpath('matlab_helper');
rerun = 1;

%% Config
n = 4;
hour = 0;
a = 209000000;
b = 0;
rho = 0.5;

corner = 'SS';
[delta1, delta2, delta3] = corner2delay(corner);

strategyPool = {'none', 'dlps', 'on-off', 'tmax', '2-bit', '4-bit'};
modePool     = {'none', 'laser', 'on-off', 'full', 'full', 'full'};
bitPool      = [0,      0,       0,        0,      2,      4];

folder = 'output/';
subfolder_task = 'task_trace/';
subfolder_comm = 'comm_trace/';

%% Rerun experiments
if rerun
    
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
end

%% Effective pjb comparison
% Baseline: no strategy;
strategy = 'none';
[energyNone, bitsNone] = getNetworkEnergyFromTrace(strategy, 0, 0);
pJbNone = energyNone/bitsNone * 1e12;

% DLPS
strategy = 'dlps';
[energyDLPS, bitsDLPS] = getNetworkEnergyFromTrace(strategy, 0, 0);
pJbDLPS = energyDLPS/bitsDLPS * 1e12;
overheadDLPS = getExecOverhead(strategy, 'none');

% ON-OFF
strategy = 'on-off';
[energyOnOff, bitsOnOff] = getNetworkEnergyFromTrace(strategy, 1, 1);
[energyOnOffLessID, ~] = getNetworkEnergyFromTrace(strategy, 0, 1);
[energyOnOffLessDW, ~] = getNetworkEnergyFromTrace(strategy, 1, 0);
pJbOnOff = energyOnOff/bitsOnOff * 1e12;
pJbOnOffLessID = energyOnOffLessID/bitsOnOff * 1e12;
pJbOnOffLessDW = energyOnOffLessDW/bitsOnOff * 1e12;
overheadOnOff = getExecOverhead(strategy, 'none');


% tmax
strategy = 'tmax';
[energyTMax, bitsTMax] = getNetworkEnergyFromTrace(strategy, 1, 1);
[energyTMaxLessID, ~] = getNetworkEnergyFromTrace(strategy, 0, 1);
[energyTMaxLessDW, ~] = getNetworkEnergyFromTrace(strategy, 1, 0);
pJbTMax = energyTMax/bitsTMax * 1e12;
pJbTMaxLessID = energyTMaxLessID/bitsTMax * 1e12;
pJbTMaxLessDW = energyTMaxLessDW/bitsTMax * 1e12;
overheadTMax = getExecOverhead(strategy, 'none');


% 2-bit
strategy = '2-bit';
[energy2Bit, bits2Bit] = getNetworkEnergyFromTrace(strategy, 1, 1);
[energy2BitLessID, ~] = getNetworkEnergyFromTrace(strategy, 0, 1);
[energy2BitLessDW, ~] = getNetworkEnergyFromTrace(strategy, 1, 0);
pJb2Bit = energy2Bit/bits2Bit * 1e12;
pJb2BitLessID = energy2BitLessID/bits2Bit * 1e12;
pJb2BitLessDW = energy2BitLessDW/bits2Bit * 1e12;
overhead2Bit = getExecOverhead(strategy, 'none');


% 4-bit
strategy = '4-bit';
[energy4Bit, bits4Bit] = getNetworkEnergyFromTrace(strategy, 1, 1);
[energy4BitLessID, ~] = getNetworkEnergyFromTrace(strategy, 0, 1);
[energy4BitLessDW, ~] = getNetworkEnergyFromTrace(strategy, 1, 0);
pJb4Bit = energy4Bit/bits4Bit * 1e12;
pJb4BitLessID = energy4BitLessID/bits4Bit * 1e12;
pJb4BitLessDW = energy4BitLessDW/bits4Bit * 1e12;
overhead4Bit = getExecOverhead(strategy, 'none');


bar1 = [(pJbNone - pJbDLPS)/pJbNone, 0, 0];

