#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)
library(ggplot2)
library(dplyr)

# Load csv file
data <- read.csv(args[1], header = FALSE)
colnames(data) <- c("link_id", "event_type", "time", "value")
data <- data %>% mutate(link_id = as.character(link_id))
data <- distinct(data)

str(data)

data %>% filter(event_type == "usage") %>% filter(time >= 0) %>% filter(time <= 4000) %>% ggplot(aes(x=time, y=value)) + geom_step() + facet_wrap(~link_id)
