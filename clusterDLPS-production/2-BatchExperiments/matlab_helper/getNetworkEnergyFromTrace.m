function [energy, bits] = getNetworkEnergyFromTrace(strategy, hasIR, hasDW)

energy = 0;
bits = 0;

trace_folder = ['output/comm_trace/', strategy, '/'];
filePattern = fullfile(trace_folder, '*.trace');
link_files = dir(filePattern);

% comm_file = ['output/dlps_events_', strategy, '.csv'];
exec_file = ['output/task_execution_', strategy, '.csv'];

for f = 1:length(link_files)
    [this_energy, this_bits] = getLinkEnergyFromTrace([trace_folder, link_files(f).name], exec_file, strategy, hasIR, hasDW);
    energy = energy + this_energy;
    bits = bits + this_bits;
end

end

