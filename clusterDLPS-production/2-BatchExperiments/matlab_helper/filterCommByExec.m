function [segments] = filterCommByExec(comm_start_end, exec_start_end)

segments = cell(size(exec_start_end, 1), 1);
for i = 1:length(segments)
    segments{i} = [exec_start_end(i,:), 0, 0, 0, 0, 0];
    for j = 1:size(comm_start_end, 1)
        if comm_start_end(j, 1) >= exec_start_end(i, 1) && comm_start_end(j, 1) <= exec_start_end(i, 2)
            segments{i} = [segments{i}; comm_start_end(j, :)];
        end
    end
end

end

