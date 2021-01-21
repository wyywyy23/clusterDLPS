function [fig, rmse_start, diff_start, rmse_end, diff_end] = plotTaskExecutionCumulative(data, fig)
% Plot cumulative task completion vs. time, return figure handle
%   TBD
actual_start_timestamps = data{8};
actual_end_timestamps = data{9};
expected_start_timestamps = data{12};
expected_end_timestamps = data{13};

x = 0:max(actual_start_timestamps) + 100;
% x = 0:4000;
ast = zeros(size(x));
est = zeros(size(x));
aet = zeros(size(x));
eet = zeros(size(x));

for i = 1:numel(actual_start_timestamps)
    ast(x >= actual_start_timestamps(i)) =  ast(x >= actual_start_timestamps(i)) + 1;
    est(x >= expected_start_timestamps(i)) =  est(x >= expected_start_timestamps(i)) + 1;
    aet(x >= actual_end_timestamps(i)) =  aet(x >= actual_end_timestamps(i)) + 1;
    eet(x >= expected_end_timestamps(i)) =  eet(x >= expected_end_timestamps(i)) + 1;
end

rmse_start = sqrt(mean((ast-est).^2));
diff_start = mean(ast-est);
rmse_end = sqrt(mean((aet-eet).^2));
diff_end = mean(aet-eet);

figure(fig);
hold on;
% plot(x, ast, 'm-');
% plot(x, est, 'c-');
plot(x, aet, 'r-');
plot(x, eet, 'b-');
xlim([0 4000])

end

