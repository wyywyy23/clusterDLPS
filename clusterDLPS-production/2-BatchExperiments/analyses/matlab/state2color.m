function [rgb] = state2color(state)

if strcmp(state, 'OFF')
    r = 34; g = 136; b = 51; % green
elseif strcmp(state, 'ON')
    r = 238; g = 102; b = 119; % red
elseif strcmp(state, 'STARTING')
    r = 204; g = 187; b = 68; % yellow
elseif strcmp(state, 'STARTED')
    r = 170; g = 51; b = 119; % purple
elseif strcmp(state, 'READY')
    r = 68; g = 119; b = 170; % blue
elseif strcmp(state, 'STANDBY')
    r = 102; g = 204; b = 238; % cyan
end
rgb = [r, g, b] / 256;
end

