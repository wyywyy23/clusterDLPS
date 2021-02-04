function [fig] = plotLinkUsage(data, fig, max_plots)
%PLOTLINKUSAGE Summary of this function goes here
%   Detailed explanation goes here

data{3} = data{3}(1:numel(data{6}));
data{4} = data{4}(1:numel(data{6}));
data{5} = data{5}(1:numel(data{6}));

status = zeros(size(data{4}));
for i = 1:numel(data{4})
    status(i) = strcmp(data{4}{i}, 'finished');
end

data = [data{3}, data{5}, data{6}, status];
data = unique(data, 'rows', 'stable');

unique_links = unique(data(:, 1));
num_links = min(numel(unique_links), max_plots);

toPlot = cell(num_links, 1);

for i = 1:numel(toPlot)
    toPlot{i} = [0, 0, 1];
end

for i = 1:size(data, 1)
    if find(unique_links == data(i, 1)) > numel(toPlot)
        continue;
    end
%     if toPlot{unique_links == data(i, 1)}(end, 3) == data(i, 4)
%        continue; 
%     end
    if toPlot{unique_links == data(i, 1)}(end, 1) == data(i, 2) %&& data(i, 4) == 0
       continue; 
    end
    toPlot{unique_links == data(i, 1)} = [toPlot{unique_links == data(i, 1)};...
                                                [toPlot{unique_links == data(i, 1)}(end,1), data(i, 3), toPlot{unique_links == data(i, 1)}(end,3)]];
    toPlot{unique_links == data(i, 1)} = [toPlot{unique_links == data(i, 1)};...
                                                [data(i, 2), data(i, 3), data(i, 4)]];
end

for i = 1:numel(toPlot)
   toPlot{i} = [toPlot{i}; [toPlot{i}(end, 1), 0, 0]]; 
end

figure(fig);
for i = 1:numel(toPlot)
    subplot(ceil(sqrt(num_links)), ceil(sqrt(num_links)), i);
    plot(toPlot{i}(:, 1), toPlot{i}(:,2));
    xlim([0 4000]); ylim([0 15e9]);
    drawnow;
end

end

