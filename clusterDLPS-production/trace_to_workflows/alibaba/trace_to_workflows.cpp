#include <iostream>
#include <vector>
#include <utility>
#include <set>
#include <map>
#include <fstream>
#include <nlohmann/json.hpp>
#include <math.h>

#include <wrench-dev.h>
#include <wrench/util/UnitParser.h>

#include "fast-cpp-csv-parser/csv.h"

std::vector<std::string> splitTaskNames(std::string task_name, std::string delimiter) {
    std::vector<std::string> split_names;
    size_t pos = 0;
    while ((pos = task_name.find(delimiter)) != std::string::npos) {
	split_names.push_back(task_name.substr(0, pos));
	task_name.erase(0, pos + delimiter.length());
    }
    split_names.push_back(task_name);
    
    return split_names;
}

class AlibabaJob : public wrench::Workflow {

    public:
	AlibabaJob* updateJob(std::string task_name, std::string instance_name, int duration, double avg_cpu, double avg_mem);
        void printPairs(); // only for debugging

    private:
	std::set<std::pair<std::string, std::string>> dependencies;
	std::set<std::pair<std::string, std::string>> task_instances; 
};

void AlibabaJob::printPairs() { // only for debugging
    std::cerr << "Dependency pairs:" << std::endl;
    for (auto& e : this->dependencies) {
	std::cerr << "{" << e.first << "," << e.second << "} ";
    }
    std::cerr << std::endl << "Task instance pairs:" << std::endl;
    for (auto& e : this->task_instances) {
        std::cerr << "{" << e.first << "," << e.second << "} ";
    }
    std::cerr << std::endl;
}

AlibabaJob* AlibabaJob::updateJob(std::string task_name, std::string instance_name, int duration, double avg_cpu, double avg_mem) {

    wrench::WorkflowTask* task = this->addTask(instance_name, duration, 1, 1, avg_mem);
    task->setAverageCPU(avg_cpu);

    /* Remove first letter and split into individual task IDs */
    std::vector<std::string> split_names;
    if (task_name.length() < 4 || (task_name.length() >= 4 && task_name.substr(0, 4).compare("task") != 0)) {
	split_names = splitTaskNames(task_name.erase(0,1), "_");
    }
    
    if (split_names.size() > 0) {
	if (split_names.size() > 1) {
            /* Update dependency map */
	    for (int i = 1; i < split_names.size(); i++) {
	        this->dependencies.insert(std::make_pair(split_names.front(), split_names.at(i)));
	    }
	}
    
        /* Update task instances map */
        this->task_instances.insert(std::make_pair(split_names.front(), instance_name));
    }
    
    return this;
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
    std::map<std::string, AlibabaJob*> jobs;

    while (task_trace.read_row(start_time, job_name, task_name, instance_name, duration, avg_cpu, avg_mem)) {
	if (jobs.empty() || jobs.find(job_name) == jobs.end()) { /* a new job */
	    AlibabaJob* job = new AlibabaJob();
	    job->setName(job_name);
	    job->setSubmittedTime(start_time);
	    jobs[job_name] = job->updateJob(task_name, instance_name, duration, avg_cpu, avg_mem);
	} else { /* existing job */
	    jobs[job_name] = jobs[job_name]->updateJob(task_name, instance_name, duration, avg_cpu, avg_mem);
	}
    }
    
    for (auto it = jobs.begin(); it != jobs.end(); ++it) {
	cerr << it->second->getName() << std::endl;
	it->second->printPairs();
    }

}
