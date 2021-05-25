close all; clear; clc;

% linkname = 'link_from_-1_-5_16_UP';
mode = {'none'};

for i = 1:numel(mode)
    linknames = dir(['../../output/comm_trace/', mode{i}, '/*.trace']);
    for l = 1:numel(linknames)
        linkname = linknames(l).name(1:end-6);
        
        filename = ['../../output/dlps_events_', mode{i}, '.csv'];
        action_trace = readActionTrace(filename);
        
        find_corner = true;
        can_plot = false;
        last_busy = 0;
        interval = [];
        comm_start_end = [];
        
        for j = 1:numel(action_trace{1})
            if ~strcmp(action_trace{3}{j}, linkname)
                continue;
            end
            if find_corner
                if action_trace{2}(j) > 0
                    if action_trace{1}(j) > last_busy
                        interval = [interval, action_trace{1}(j) - last_busy];
                    end
                    corner = [action_trace{1}(j), -1.5];
                    find_corner = false;
                    can_plot = false;
                end
            else
                if action_trace{2}(j) == 0
                    last_busy = action_trace{1}(j);
                    edge = [action_trace{1}(j) - corner(1), 1];
                    find_corner = true;
                    can_plot = true;
                end
            end
            if can_plot
                comm_start_end = [comm_start_end; corner(1), corner(1) + edge(1)];
            end
        end
        if ~isempty(comm_start_end)
            comm_start_end = unionOfIntervals(comm_start_end);
            
            task_trace = readTaskExecutionTrace(['../../output/task_execution_', mode{i}, '.csv']);
            exec_start_end = unionOfIntervals([task_trace{6}, task_trace{11}]);
            
            saving = 1 - (sum(comm_start_end(:,2) - comm_start_end(:,1)) + size(comm_start_end,1)*0.002)/sum(exec_start_end(:,2) - exec_start_end(:,1));
        end
    end
end