function [energy, bits] = getLinkEnergyFromCommsIdles(comms, idles, strategy, hasIR, hasDW)

bits = 0;
energy = 0;

if ~isempty(comms)
    bits = sum(comms(:, 5)*8);
end

switch strategy
    case 'dlps'
        for i = 1:size(comms, 1)
            energy = energy + (comms(i, 2) - comms(i, 1)) * util2Power(1);
        end
        for i = 1:size(idles, 1)
            idle_time = idles(i, 2) - idles(i, 1);
            thresh1 = idles(i, 3);
            ready_time = min(idle_time, thresh1);
            energy = energy + readyPower() * ready_time + standbyPower() * max(0, idle_time - ready_time);
        end
    case 'on-off'
        for i = 1:size(comms, 1)
            if hasIR
                util = comms(i, 3)/87300000000;
            else
                util = 1;
            end
            if hasDW
                comms(i, 1) = comms(i, 1) + comms(i, 4) * (comms(i, 2) - comms(i, 1));
            end
            energy = energy + (comms(i, 2) - comms(i, 1)) * util2Power(util);
        end
    otherwise
        for i = 1:size(comms, 1)
            if hasIR
                util = comms(i, 3)/87300000000;
            else
                util = 1;
            end
            if hasDW
                comms(i, 1) = comms(i, 1) + comms(i, 4) * (comms(i, 2) - comms(i, 1));
            end
            energy = energy + (comms(i, 2) - comms(i, 1)) * util2Power(util);
        end
        for i = 1:size(idles, 1)
            idle_time = idles(i, 2) - idles(i, 1);
            thresh1 = idles(i, 3);
            thresh2 = idles(i, 4);
            ready_time = min(idle_time, thresh1);
            standby_time = max(0, min(idle_time, thresh2) - ready_time);
            energy = energy + readyPower() * ready_time + standbyPower() * standby_time;
        end        
end
        

end

