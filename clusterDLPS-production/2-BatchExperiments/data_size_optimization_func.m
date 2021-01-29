function rmse = data_size_optimization_func(x, hour, n)
%DATA_SIZE_OPTIMIZATION_FUNC Summary of this function goes here
%   Detailed explanation goes here

param = [x.param_1, x.param_2] * 1000000;
rmse = 0;

while hour < 1
    
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
    rmse = rmse + sqrt(mean((data{9}-data{13}).^2));
    hour = hour + 1;
    
end

end

