#include <iostream>
#include <iomanip>
#include <utility>
#include <map>
#include <fstream>
#include <math.h>

#include "fast-cpp-csv-parser/csv.h"

void dump_job_count(std::map<int, unsigned long> job_count, std::map<int, unsigned long> dependent_job_count, std::string output_path) {

    /* Write a csv file*/
    std::ofstream file;
    file.exceptions(std::ofstream::failbit | std::ofstream::badbit);
    try {
	file.open(output_path);
	for (auto it = job_count.begin(); it !=job_count.end(); ++it) {
	    file << it->first << "," << it->second << ",";
	    if (dependent_job_count.find(it->first) != dependent_job_count.end())
		file << dependent_job_count[it->first] << std::endl;
	    else
		file << 0 << std::endl;
	}
	file.close();
    } catch (const std::ofstream::failure &e) {
	throw std::invalid_argument("Cannot open file for output!");
    }

}

int main(int argc, char **argv) {

    if (argc != 1) {
	std::cerr << "Usage: " << argv[0] << std::endl;
	exit(1);
    }
    

    std::string trace_file_path = "trace/batch_instance.csv";
    std::string output_path = "task_count.csv";

    std::cerr << "Trace file:\t" << trace_file_path << std::endl;
    std::cerr << "Output Path:\t" << output_path << std::endl;

    const int num_instance_column = 14;

    /* Open instance trace */
    io::CSVReader<num_instance_column> task_trace(trace_file_path);
    
    std::string instance_name;
    std::string task_name;
    std::string job_name;
    std::string task_type;
    std::string status;
    double start_time;
    double end_time;
    std::string machine_id;
    long sequence_number;
    long total_sequence_number;
    double avg_cpu;
    double max_cpu;
    double avg_mem;
    double max_mem;

    /* Initiate a map of workflows (jobs) as <jobID, workflow>*/
    std::map<int, unsigned long> job_count;
    std::map<int, unsigned long> dependent_job_count;

    long lines_read = 0;
    while (task_trace.read_row(instance_name, task_name, job_name, task_type, status, start_time, end_time, machine_id, sequence_number, total_sequence_number, avg_cpu, max_cpu, avg_mem, max_mem)) {

	lines_read++;
	bool verbose = lines_read % 10000 == 0 ? true : false;
	if (verbose) {
	    std::cerr << "Read " << std::setw(10) << (double) lines_read / 1351255775 * 100 << "\% of file...\r";
	}

	int hour = std::floor(start_time / 3600);
	if (job_count.find(hour) == job_count.end()) job_count[hour] = 1;
	else job_count[hour] = job_count[hour] + 1;

	if (task_name.length() < 4 || (task_name.length() >= 4 && task_name.substr(0, 4).compare("task") != 0)) {
	    if (dependent_job_count.find(hour) == dependent_job_count.end()) dependent_job_count[hour] = 1;
            else dependent_job_count[hour] = dependent_job_count[hour] + 1;
	}
    }
    

    std::cerr << "Read " << std::setw(10) << (double) lines_read / 1351255775 * 100 << "\% of file...\r";

    std::cerr << std::endl;

    dump_job_count(job_count, dependent_job_count, output_path);

    return 0;
}
