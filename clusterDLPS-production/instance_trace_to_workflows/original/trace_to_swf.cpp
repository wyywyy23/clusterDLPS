#include <iostream>
#include <iomanip>
#include <utility>
#include <fstream>
#include <math.h>
#include <set>

#include "fast-cpp-csv-parser/csv.h"
#include "helper/helper.h"

int main(int argc, char **argv) {

    if (argc != 1) {
	std::cerr << "Usage: " << argv[0] << std::endl;
	exit(1);
    }
    
    std::string trace_file_path = "trace/container_meta.csv";
    std::string output_path = "output/swf/";

    std::cerr << "Trace file:\t" << trace_file_path << std::endl;
    std::cerr << "Output Path:\t" << output_path << std::endl;

    int max_num_machine = 4096;
    const int num_container_column = 8;

    /* Open instance trace */
    io::CSVReader<num_container_column> container_trace(trace_file_path);
    std::ofstream file;
    file.exceptions(std::ofstream::failbit | std::ofstream::badbit);
    try {
        file.open(output_path + "container_trace.swf");
    } catch (const std::ofstream::failure &e) {
        throw std::invalid_argument("Cannot open file for output!");
    }
   
    std::string container_id;
    std::string machine_id;
    long start_time;
    std::string app_du;
    std::string status;
    long avg_cpu;
    long max_cpu;
    double avg_mem;

    long lines_read = 0;
    long valid_containers = 0;
    long wait_time = 0;
    long run_time = 864000;
    std::set<std::string> containers;

    while (container_trace.read_row(container_id, machine_id, start_time, app_du, status, avg_cpu, max_cpu, avg_mem)) {

	lines_read++;
	bool verbose = lines_read % 100 == 0 ? true : false;
	if (verbose) {
	    std::cerr << "Read " << std::setw(10) << (double) lines_read / 370540 * 100 << "\% of file...\t"
		  << "# of containers: " << std::setw(8) << valid_containers <<"\r";
	}

	if (containers.find(container_id) != containers.end()) continue;

    	/* Get (long) static host id from machine_id */
    	std::vector<std::string> machine_id_split = splitString(machine_id, "_");
    	long host_id = std::stol(machine_id_split.back()) - 1;

	/* Check machine in range */
	if (host_id >= max_num_machine) continue;

	/* Skip failed task */
	if (status != "started") continue;

	file << valid_containers++ << " "
             << start_time << " "
             << wait_time << " "
             << run_time << " "
             << avg_cpu/100 << " "
             << "-1 "
             << "-1 "
             << max_cpu/100 << " "
             << "-1 "
             << "-1 "
             << "-1 "
             << "1 "
             << "1 "
             << "1 "
             << "1 "
             << "-1 "
             << "-1 "
             << "-1 "
             << host_id << std::endl;
             
	containers.insert(container_id);
    }
    file.close();
    std::cerr << std::endl;

    return 0;
}