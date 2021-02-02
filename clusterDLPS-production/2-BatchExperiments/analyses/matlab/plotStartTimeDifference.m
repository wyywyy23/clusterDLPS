function [fig, p] = plotStartTimeDifference(data, fig)
% Plot cumulative task completion vs. time, return figure handle
%   TBD
execution_time = data{9} - data{8};
time_difference = data{12} - data{8};
num_of_parents = data{14};

figure(fig);
hold on;

toPlotX = execution_time;
toPlotY = time_difference;
X = toPlotX(toPlotY >= 0);
Y = toPlotY(toPlotY >= 0);
% scatter(X, Y);

histogram(Y);
[norm_trunc, phat, phat_ci]  = fitdist_ntrunc(Y, [0, inf]);
% X = execution_time(time_difference > 0);
% Y = time_difference(time_difference > 0);
% N = num_of_parents(time_difference > 0);
% scatter(N, Y)
% 
% p = polyfit(N, Y, 1);
% plot(X, p(1) * X + p(2));

end

