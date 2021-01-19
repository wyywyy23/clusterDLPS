function [fig, h] = plotTaskExecutionCumulative(data, fig)
% Plot cumulative task completion vs. time, return figure handle
%   TBD

execution_end_timestamps = data{9};
expected_end_timestamps = data{12};

x = 0:0.01:max(execution_end_timestamps);
y1 = zeros(size(x));
y2 = zeros(size(x));

for i = 1:numel(execution_end_timestamps)
   y1(x >= execution_end_timestamps(i)) =  y1(x >= execution_end_timestamps(i)) + 1;
   y2(x >= expected_end_timestamps(i)) =  y2(x >= expected_end_timestamps(i)) + 1;
end

figure(fig);
hold on;
plot(x, y1, 'r-');
plot(x, y2, 'b-');

end

