function [data] = readLinkUsageTrace(trace_file)
% Read task execution trace and return data
%   TBD

rid = fopen(trace_file, 'r');
if rid ~= -1
    
    data = textscan(rid, 'link_from_%f_%f_%f_%s %s %f %f %f', 'Delimiter', ',');
    
    fclose(rid);
end

end

