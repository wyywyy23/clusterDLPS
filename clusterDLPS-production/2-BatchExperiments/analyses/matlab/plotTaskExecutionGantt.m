function [fig, h] = plotTaskExecutionGantt(data, fig, jobs)
% Plot task execution gannt chart vs. time, return figure handle
%   TBD
job_id = data{1};
instance_id = data{2};
host_id = data{3};
cores_total = data{4};
% cores_allocated = data{5};
read_start_timestamps = data{6};
read_end_timestamps = data{7};
execution_start_timestamps = data{8};
execution_end_timestamps = data{9};
write_start_timestamps = data{10};
write_end_timestamps = data{11};

if isempty(jobs)
    jobs = unique(job_id);
end

figure(fig);
hold on;

%% Map host id to host # of cores
[unique_hosts, idx, ~] = unique(host_id);
map_host_to_cores = [unique_hosts, cores_total(idx)];

%% Map host id to core offset
map_host_to_core_offset = zeros(size(map_host_to_cores));
map_host_to_core_offset(:, 1) = unique_hosts;
for i = 2:size(map_host_to_core_offset, 1)
    map_host_to_core_offset(i, 2) = sum(map_host_to_cores(1:i-1, 2));
end

%% Start time and end time list for all hosts
start_time_record = NaN(numel(unique_hosts), numel(instance_id));
end_time_record = NaN(numel(unique_hosts), numel(instance_id));
for i = 1:numel(instance_id)
    start_time_record(unique_hosts == host_id(i), i) = read_start_timestamps(i);
    end_time_record(unique_hosts == host_id(i), i) = write_end_timestamps(i);
end

%% Number of instances starting at the same time
%same_start_time_record = zeros(numel(unique_hosts), numel(unique(read_start_timestamps)));

%%
core_occupation_start_record = NaN(sum(map_host_to_cores(:,2)), numel(instance_id));
core_occupation_end_record = NaN(sum(map_host_to_cores(:,2)), numel(instance_id));

%% Derive y loc for each trace line
for i = 1:numel(instance_id)
    if ismember(job_id(i), jobs)
        this_start_time = read_start_timestamps(i);
        this_end_time = write_end_timestamps(i);
        %        same_start_time_record(unique_hosts == host_id(i), unique(read_start_timestamps) == this_start_time)...
        %            = same_start_time_record(unique_hosts == host_id(i), unique(read_start_timestamps) == this_start_time) + 1;
        %        occupied_cores = numel(find(this_start_time > start_time_record(unique_hosts == host_id(i), :)))...
        %            - numel(find(this_start_time > end_time_record(unique_hosts == host_id(i), :)));
        for j = 1:map_host_to_cores(unique_hosts == host_id(i), 2)
            this_core_ok = 1;
            for k = 1:size(core_occupation_start_record, 2)
                if (this_start_time >= core_occupation_start_record(map_host_to_core_offset(unique_hosts == host_id(i), 2) + j, k) && this_start_time < core_occupation_end_record(map_host_to_core_offset(unique_hosts == host_id(i), 2) + j, k))...
                        || (this_end_time > core_occupation_start_record(map_host_to_core_offset(unique_hosts == host_id(i), 2) + j, k) && this_end_time <= core_occupation_end_record(map_host_to_core_offset(unique_hosts == host_id(i), 2) + j, k))
                    this_core_ok = 0;
                    break
                end
            end
            if this_core_ok
                core_occupation_start_record(map_host_to_core_offset(unique_hosts == host_id(i), 2) + j, i) = this_start_time;
                core_occupation_end_record(map_host_to_core_offset(unique_hosts == host_id(i), 2) + j, i) = this_end_time;
                y_loc = map_host_to_core_offset(unique_hosts == host_id(i), 2) + j;
                break
            end
        end
        %        y_loc = occupied_cores + map_host_to_core_offset(unique_hosts == host_id(i), 2)...
        %            + same_start_time_record(unique_hosts == host_id(i), unique(read_start_timestamps) == this_start_time);
        if read_start_timestamps(i) < read_end_timestamps(i)
            figure(fig);
            plot([read_start_timestamps(i), read_end_timestamps(i)], [y_loc, y_loc], '.-b');
        end
        if execution_start_timestamps(i) < execution_end_timestamps(i)
            figure(fig);
            plot([execution_start_timestamps(i), execution_end_timestamps(i)], [y_loc, y_loc], '.-r');
        end
        if write_start_timestamps(i) < write_end_timestamps(i)
            figure(fig);
            plot([write_start_timestamps(i), write_end_timestamps(i)], [y_loc, y_loc], '.-g');
        end
%                 drawnow;
    end
end

end

