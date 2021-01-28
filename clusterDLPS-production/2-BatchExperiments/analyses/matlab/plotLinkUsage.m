function [fig] = plotLinkUsage(data, fig, max_plots)
%PLOTLINKUSAGE Summary of this function goes here
%   Detailed explanation goes here

data{4} = padarray(data{4}, length(data{3}) - length(data{4}), 'post');
data{5} = padarray(data{5}, length(data{3}) - length(data{5}), 'post');

data = [data{3}, data{4}, data{5}];
data = unique(data, 'rows');

unique_links = unique(data(:, 1));
num_links = min(numel(unique_links), max_plots);

toPlot = cell(num_links, 1);

for i = 1:numel(toPlot)
    toPlot{i} = [0, 0];
end

for i = 1:size(data, 1)
    if find(unique_links == data(i, 1)) > numel(toPlot)
        continue;
    end
    toPlot{unique_links == data(i, 1)}(end, 2) = data(i, 3);
    toPlot{unique_links == data(i, 1)} = [toPlot{unique_links == data(i, 1)};...
                                                [data(i, 2), data(i, 3)]];
    toPlot{unique_links == data(i, 1)} = [toPlot{unique_links == data(i, 1)};...
                                                [data(i, 2), 0]];
end

figure(fig);
for i = 1:numel(toPlot)
    subplot(ceil(sqrt(num_links)), ceil(sqrt(num_links)), i);
    plot(toPlot{i}(:, 1), toPlot{i}(:,2));
    drawnow;
end

end

