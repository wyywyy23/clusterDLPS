#include <iostream>
#include <vector>
#include <fstream>
#include <nlohmann/json.hpp>

#include "fast-cpp-csv-parser/csv.h"

int main() {

    int num_machine = 4;

    const int num_task_column = 7;
    const int num_instance_column = 7;

    io::CSVReader<num_task_column> task_trace(
	    "trace/" + std::to_string(num_machine) + "_sample/batch_task.csv");
    
    double task_time;
    std::string job_name;
    std::string task_name;
    int task_length;
    double req_cpu;
    double req_mem;
    int num_instance;

    while(task_trace.read_row(task_time, job_name, task_name, task_length, req_cpu, req_mem, num_instance)) {
	std::cout << task_time    << " "
                  << job_name     << " "
                  << task_name    << " "
                  << task_length  << " "
                  << req_cpu      << " "
                  << req_mem      << " "
                  << num_instance << std::endl;
    }

}
