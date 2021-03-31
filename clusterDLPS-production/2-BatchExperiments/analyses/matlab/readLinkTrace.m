function [data] = readLinkTrace(trace_file)

rid = fopen(trace_file, 'r');
if rid ~= -1
    
    data = textscan(rid, '%f %d %f %f', 'delimiter', ',');
    
    fclose(rid);
end

end