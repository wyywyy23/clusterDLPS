#include <iostream>
#include <vector>
#include <map>
#include <fstream>
#include <nlohmann/json.hpp>
#include <math.h>

#include <wrench-dev.h>
#include <wrench/util/UnitParser.h>

#include "fast-cpp-csv-parser/csv.h"

wrench::Workflow* updateJob(wrench::Workflow* job,
                            std::string task_name, std::string instance_name,
                            int duration, double avg_cpu, double avg_mem) {
    job->setName(job->getName() + "_updated");
    return job;
}

int main(int argc, char **argv) {

    if (argc != 2) {
	std::cerr << "Usage: " << argv[0] << "<number of machines>" << std::endl;
	exit(1);
    }
    
    int num_machine = 4; 
    try {
	num_machine = std::atoi(argv[1]);
    } catch (std::invalid_argument &e) {
	std::cerr << "Invalid number of machines. Must be 2^{2:12}." << std::endl;
	exit(1);
    }

    if (log2(num_machine) < 2 || log2(num_machine) > 12 || log2(num_machine) != (int) log2(num_machine)) {
	std::cerr << "Invalid number of machines. Must be 2^{2:12}." << std::endl;
        exit(1);
    }

    const int num_task_column = 7;
    const int num_instance_column = 7;

    /* Open instance trace */
    io::CSVReader<num_task_column> task_trace(
	    "trace/" + std::to_string(num_machine) + "_sample/batch_instace.csv");
    
    double start_time;
    std::string job_name;
    std::string task_name;
    std::string instance_name;
    int duration;
    double avg_cpu;
    double avg_mem;

    /* Initiate a map of workflows (jobs) as <jobID, workflow>*/
    std::map<std::string, wrench::Workflow*> jobs;

    while (task_trace.read_row(start_time, job_name, task_name, instance_name, duration, avg_cpu, avg_mem)) {
	
	if (jobs.empty() || jobs.find(job_name) == jobs.end()) { /* a new job */
	    wrench::Workflow* job = new wrench::Workflow();
	    job->setName(job_name);
	    job->setSubmittedTime(start_time);
	    jobs[job_name] = updateJob(job, task_name, instance_name, duration, avg_cpu, avg_mem);
	}
    }

    for (auto it = jobs.begin(); it != jobs.end(); ++it) {
	cerr << it->first << "\t" << it->second->getName() << "\t" << it->second->getSubmittedTime() << std::endl;
    }

}
