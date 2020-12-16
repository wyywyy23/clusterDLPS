#!/bin/bash
div ()  # Arguments: dividend and divisor
{
        if [ $2 -eq 0 ]; then echo division by 0; exit; fi
        local p=12                            # precision
        local c=${c:-0}                       # precision counter
        local d=.                             # decimal separator
        local r=$(($1/$2)); echo -n $r        # result of division
        local m=$(($r*$2))
        [ $c -eq 0 ] && [ $m -ne $1 ] && echo -n $d
        [ $1 -eq $m ] || [ $c -eq $p ] && echo && return
        local e=$(($1-$m))
        c=$(($c+1))
        div $(($e*10)) $2
}

TRACE_DIR="./original_sample/"
OUTPUT_DIR="./"

for nMachines in 4 8 16 32 64 128 256 512 1024 2048 4096
do	
    LOAD_FACTOR=$( div ${nMachines} 4096 )
    OUTPUT_DIR="./${nMachines}_sample"
    mkdir -p ${OUTPUT_DIR}
    spar ${OUTPUT_DIR} --trace-dir ${TRACE_DIR} --load-factor ${LOAD_FACTOR}
done	
