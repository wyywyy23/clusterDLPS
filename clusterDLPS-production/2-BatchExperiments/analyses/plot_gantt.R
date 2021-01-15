#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)
library(ggplot2)

# Load csv file
data <- read.csv(args[1], header = FALSE)
colnames(data) <- c("job_id", "task_id", "hostname", "total_cores", "allocated_cores", "read_start", "read_end", "compute_start", "compute_end", "write_start", "write_end")

head(data)
