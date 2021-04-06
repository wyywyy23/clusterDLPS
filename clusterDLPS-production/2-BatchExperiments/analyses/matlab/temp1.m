close all; clear; clc;

linkname = 'link_from_0_-1_0_UP';
mode = {'none', 'on-off', 'laser', 'full'};
figure;

for i = 1:numel(mode)
    % plot action start and end
    filename = ['../../output/dlps_events_', mode{i}, '.csv'];
    action_trace = readActionTrace(filename);
    subplot(numel(mode), 1, i); hold on;
    
    find_corner = true;
    can_plot = false;
    last_busy = 0;
    interval = [];
    for j = 1:numel(action_trace{1})
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
            rectangle('Position', [corner, edge], 'FaceColor', 'k', 'EdgeColor', 'k');
        end
    end
    
    xlim([0 4000])
    
%     % plot link start and end
%     filename = ['../../output/comm_trace/', mode{i}, '/', linkname, '.trace'];
%     link_trace = readLinkTrace(filename);
%     subplot(numel(mode), 1, i); hold on;
%     
%     find_corner = true;
%     can_plot = false;
%     last_busy = 0;
%     interval = [];
%     for j = 1:numel(link_trace{1})
%         if find_corner
%             if link_trace{2}(j) > 0
%                 if link_trace{1}(j) > last_busy
%                     interval = [interval, link_trace{1}(j) - last_busy];
%                 end
%                 corner = [link_trace{1}(j), -3];
%                 find_corner = false;
%                 can_plot = false;
%             end
%         else
%             if link_trace{2}(j) == 0
%                 last_busy = link_trace{1}(j);
%                 edge = [link_trace{1}(j) - corner(1), 1];
%                 find_corner = true;
%                 can_plot = true;
%             end
%         end
%         if can_plot
%             rectangle('Position', [corner, edge], 'FaceColor', 'k', 'EdgeColor', 'k');
%         end
%     end
    
%     % plot 
%     filename = ['../../output/comm_trace/', mode{i}, '/', linkname, '.trace'];
%     link_trace = readLinkTrace(filename);
%     link_trace{3} = [link_trace{3}(2:end); 0];
%     link_trace{4} = [link_trace{4}(2:end); 0];
%     subplot(numel(mode), 1, i); hold on;
%     
%     for j = 1:numel(link_trace{1})
%         if j == 1
%             corner = [link_trace{1}(j), -3];
%             color = state2color(link_trace{2}{j});
%         else
%             edge = [link_trace{1}(j) - corner(1), 1];
%             rectangle('Position', [corner, edge], 'FaceColor', color, 'EdgeColor', color);
%             corner = [link_trace{1}(j), -3];
%             color = state2color(link_trace{2}{j});
%         end
%     end
    
end