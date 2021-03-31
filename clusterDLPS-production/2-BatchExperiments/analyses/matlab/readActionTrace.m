function [data] = readActionTrace(trace_file)

rid = fopen(trace_file, 'r');
if rid ~= -1
    
    data = textscan(rid, '%f %d', 'delimiter', ',');
    
    fclose(rid);
end

end