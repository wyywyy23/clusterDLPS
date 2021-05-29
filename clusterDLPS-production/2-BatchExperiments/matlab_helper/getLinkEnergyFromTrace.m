function [energy, bits] = getLinkEnergyFromTrace(comm_file, exec_file, strategy, hasIR, hasDW)

energy = 0;
bits = 0;

linkTrace = readLinkTrace(comm_file);
execTrace = readExecTrace(exec_file);

comm_start_end = linkTrace2StartEndPair(linkTrace);
exec_start_end = unionOfIntervals([execTrace{6}, execTrace{11}]);

segments = filterCommByExec(comm_start_end, exec_start_end);

for i = 1:length(segments)
    [this_energy, this_bits] = getLinkEnergyFromSegment(segments{i}, strategy, hasIR, hasDW);
    energy = energy + this_energy;
    bits = bits + this_bits;
end

end

