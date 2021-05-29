function [overhead] = getExecOverhead(mode1, mode2)

overhead = 0;

trace1 = ['output/task_execution_' mode1, '.csv'];
data1 = readTaskExecutionTrace(trace1);
exec_time1 = data1{11} - data1{6};
finish_time1 = data1{11};

[~, I] = sort(data1{2});
exec_time1 = exec_time1(I);
finish_time1 = finish_time1(I);

trace2 = ['output/task_execution_' mode2, '.csv'];
data2 = readTaskExecutionTrace(trace2);
exec_time2 = data2{11} - data2{6};
finish_time2 = data2{11};

[~, I] = sort(data2{2});
exec_time2 = exec_time2(I);
finish_time2 = finish_time2(I);

overhead = (exec_time1 - exec_time2)./exec_time2;
overhead(overhead < 0) = 0;
end

