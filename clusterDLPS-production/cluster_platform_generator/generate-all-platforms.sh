#!/bin/bash

for nMachines in 4 8 16 32 64 128 256 512 1024 2048 4096
do
	./generate-platform fat_tree ${nMachines}
done	
