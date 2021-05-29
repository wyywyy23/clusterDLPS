function [delay1, delay2, delay3] = corner2delay(corner)

switch corner
    case 'FF'
        delay1 = 1e-9;
        delay2 = 10e-9;
        delay3 = 1e-6;
    case 'FS'
        delay1 = 1e-9;
        delay2 = 10e-9;
        delay3 = 1e-3;
    case 'SF'
        delay1 = 10e-9;
        delay2 = 100e-9;
        delay3 = 1e-6;
    case 'SS'
        delay1 = 10e-9;
        delay2 = 100e-9;
        delay3 = 1e-3;
end

end

