function [new_comm_start_end] = linkTrace2StartEndPair(link_trace)

comm_start_end = [];
action_ids = link_trace{2};
for i = 1:length(action_ids)
    if action_ids(i) ~= -1
        idx = find(action_ids == action_ids(i));
        comm_start_end = [comm_start_end; link_trace{1}(idx(1)), link_trace{5}(idx(2)),...
            link_trace{6}(idx(2)), link_trace{7}(idx(1)), link_trace{8}(idx(2)), link_trace{9}(idx(2)), link_trace{10}(idx(2))];
        action_ids(idx) = -1;
    end
end

new_comm_start_end = [];
if ~isempty(comm_start_end)
    start_end = comm_start_end(:,1:2);
    start_end = unionOfIntervals(start_end);
    
    for i = 1:size(start_end, 1)
        idx1 = find(comm_start_end(:, 1) == start_end(i, 1));
        idx2 = find(comm_start_end(:, 2) == start_end(i, 2));
        new_comm_start_end = [new_comm_start_end; start_end(i, :),...
            comm_start_end(idx2(1), 3), comm_start_end(idx1(1), 4), comm_start_end(idx2(1), 5), comm_start_end(idx2(1), 6), comm_start_end(idx2(1), 7)];
    end
end

end

