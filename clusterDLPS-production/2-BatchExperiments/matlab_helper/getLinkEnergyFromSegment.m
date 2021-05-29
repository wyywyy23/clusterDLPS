function [energy, bits] = getLinkEnergyFromSegment(start_end, strategy, hasIR, hasDW)
initial = start_end(1, 1);
final = start_end(1, 2);
start_end(1, :) = [];

idles = [];
comms = [];

if ~isempty(start_end)
    idles = [initial, start_end(1, 1), 0, 0];
    for i = 1:size(start_end, 1)
        comms = [comms; start_end(i, [1:4, 7])];
        if i > 1
            idles = [idles; start_end(i-1, 2), start_end(i, 1), start_end(i-1, 5:6)];
        end
    end
    idles = [idles; start_end(i, 2), final, start_end(i, 5:6)];
else
    idles = [initial, final, 0, 0];
end

switch strategy
    case 'none'
        energy = (final - initial) * util2Power(1);
        [~, bits] = getLinkEnergyFromCommsIdles(comms, idles, strategy, hasIR, hasDW);
    otherwise
        [energy, bits] = getLinkEnergyFromCommsIdles(comms, idles, strategy, hasIR, hasDW);
end

end

