function rmse = data_size_optimization_func(x, hour, n)
%DATA_SIZE_OPTIMIZATION_FUNC Summary of this function goes here
%   Detailed explanation goes here

param = [x.param_1, x.param_2] * 1000000;
rmse = 0;

hour1 = hour + 1;

while hour < hour1
    
    command = ['./wyy_simulator platforms/cluster_4096_machines_FAT_TREE.xml ',...
        '../instance_trace_to_workflows/original/output/swf/container_trace.swf ',...
        '../instance_trace_to_workflows/original/output/workflows_for_data_size_optimization/',...
        num2str(hour), '-', num2str(hour+1), ' ', num2str(n), ' ',...
        num2str(param(1)), ' ', num2str(param(2)), ' ',...
        'static ',...
        '--log=wyy_wms.th:info --log=wyy_wms.fmt:%m ',...
        '--log=wyy_wms.app:file:output/task_execution.csv ',...
        '--cfg=contexts/guard-size:0'];
%     [status, ~, ~] = jsystem(command, 'noshell');
    [status, ~] = system(command);
    
    if status ~= 0
        ME = MException('Simulator returned error! Aborting optimization.');
        throw(ME);
    end
    
    data = readTaskExecutionTrace('output/task_execution.csv');
    actual_end_timestamps = data{9};
    expected_end_timestamps = data{13};
    
    x = 0:max(max(actual_end_timestamps), max(expected_end_timestamps));
    aet = zeros(size(x));
    eet = zeros(size(x));

    for i = 1:numel(actual_end_timestamps)
        aet(x >= actual_end_timestamps(i)) =  aet(x >= actual_end_timestamps(i)) + 1;
        eet(x >= expected_end_timestamps(i)) =  eet(x >= expected_end_timestamps(i)) + 1;
    end
    
    rmse = rmse + sqrt(mean((aet-eet).^2));
%    rmse = sum(aet > eet) +...
%            max(actual_end_timestamps) - max(expected_end_timestamps);
    hour = hour + 1;
    
end

end

