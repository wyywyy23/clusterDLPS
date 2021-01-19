function [data] = readTaskExecutionTrace(trace_file)
% Read task execution trace and return data
%   TBD

rid = fopen(trace_file, 'r');
if rid ~= -1
    
    data = textscan(rid, 'j_%d %s node-%d %d %d %f %f %f %f %f %f %f',...
                         'delimiter', ',');
    
    fclose(rid);
end

end

