function [data] = readTaskExecutionTrace(trace_file)
% Read task execution trace and return data
%   TBD

rid = fopen(trace_file, 'r');
if rid ~= -1
    
    data = textscan(rid, '%s %s node-%d %d %d %f %f %f %f %f %f %f %f %d',...
                         'delimiter', ',');
    
    fclose(rid);
end

for i = 1:size(data,2)
    data{i} = data{i}(1:numel(data{end}));

end

