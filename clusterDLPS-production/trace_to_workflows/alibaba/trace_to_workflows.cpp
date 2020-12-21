#include <iostream>
#include <vector>
#include <utility>
#include <set>
#include <map>
#include <fstream>
#include <nlohmann/json.hpp>
#include <math.h>
#include <time.h>

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

double generateFileSize(double exe_time) { // File size in KB
    return exe_time * 50000000;
}

class AlibabaJob : public wrench::Workflow {

    public:
	AlibabaJob* updateJob(std::string task_name, std::string instance_name, int duration, double avg_cpu, double avg_mem);
	std::set<std::pair<std::string, std::string>> getDependencyMap() {
		return this->dependencies; }
	std::multimap<std::string, std::string> getTaskInstanceMap() {
		return this->task_instances; }
        void printPairs(); // only for debugging

    private:
	std::set<std::pair<std::string, std::string>> dependencies;
	std::multimap<std::string, std::string> task_instances; 
};

void AlibabaJob::printPairs() { // only for debugging
    std::cerr << "Dependency pairs:" << std::endl;
    for (auto it = this->dependencies.begin(); it != this->dependencies.end(); ++it) {
	std::cerr << "{" << it->first << "," << it->second << "} ";
    }
    std::cerr << std::endl << "Task instance pairs:" << std::endl;
    for (auto it = this->task_instances.begin(); it != this->task_instances.end(); ++it) {
        std::cerr << "{" << it->first << "," << it->second << "} ";
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
        this->task_instances.emplace(split_names.front(), instance_name);
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

    time_t rawtime;
    struct tm* timeinfo;

    const int num_task_column = 7;
    const int num_instance_column = 7;

    /* Open instance trace */
    io::CSVReader<num_task_column> task_trace(
	    "trace/" + std::to_string(num_machine) + "_sample/batch_instace.csv");
    
    double start_time;
    std::string job_name;
    std::string task_name;
    std::string instance_name;
    double duration;
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
    
    for (auto itj = jobs.begin(); itj != jobs.end(); ++itj) {
	 cerr << "Generating JSON for " <<itj->first << " ..." << std::endl;
	/* Add an output file to each task instance */
	auto tasks = itj->second->getTaskMap();
	for (auto itt = tasks.begin(); itt != tasks.end(); ++itt) {
	    wrench::WorkflowFile* output_file = nullptr;
	    std::string file_name = itt->first + "_output";
	    try {
		output_file = itj->second->getFileByID(file_name);
		std::cerr << "Got duplicate file!" << std::endl;
	    } catch (const std::invalid_argument &ia) {
	        output_file = itj->second->addFile(file_name, generateFileSize(itt->second->getFlops()));
		itt->second->addOutputFile(output_file);
	    }
	}

        /* Add task dependencies according to the dependency map */
	auto d_map = itj->second->getDependencyMap();
	auto t_map = itj->second->getTaskInstanceMap();
	for (auto itd = d_map.begin(); itd != d_map.end(); ++itd) {
	    auto child_range = t_map.equal_range(itd->first);
	    auto parent_range = t_map.equal_range(itd->second);
	    for (auto itc = child_range.first; itc != child_range.second; ++itc) {
		for (auto itp = parent_range.first; itp != parent_range.second; ++itp) {
		    tasks[itc->second]->addInputFile(itj->second->getFileByID(itp->second + "_output"));
		}
	    }
	}

	/* Add an input file to each root task */
	auto entry_tasks = itj->second->getEntryTaskMap();
	for (auto ite = entry_tasks.begin(); ite != entry_tasks.end(); ++ite) {
	    wrench::WorkflowFile* input_file = nullptr;
	    std::string file_name = ite->first + "_input";
	    try {
		input_file = itj->second->getFileByID(file_name);
		std::cerr << "Got duplicate file!" << std::endl;
	    } catch (const std::invalid_argument &ia) {
		input_file = itj->second->addFile(file_name, generateFileSize(ite->second->getFlops()));
		ite->second->addInputFile(input_file);
	    }
	}

	/* Create JSON structure*/
	nlohmann::json j;
	j["name"] = itj->first;
	j["description"] = "This job contains " + std::to_string(itj->second->getNumberOfTasks()) + " tasks.";
	time (&rawtime);
	timeinfo = localtime (&rawtime);
	char buffer [80];
	strftime(buffer, 80, "%FT%T%z", timeinfo);
	j["createdAt"] = buffer;
	j["schemaVersion"] = "1.0";
	j["author"] = {{"name", "Yuyang Wang"}, {"email", "wyy@ece.ucsb.edu"}, {"institution", "University of California, Santa Barbara"}, {"country", "US"}};
	j["wms"] = {{"name", "none"}, {"url", "none"}, {"version", "none"}};

	nlohmann::json j_workflow = nlohmann::json::object();
	j_workflow["makespan"] = -1;
	j_workflow["executedAt"] = itj->second->getSubmittedTime();
	nlohmann::json j_machines = nlohmann::json::array();
	for (int idx_machine = 0; idx_machine < num_machine; idx_machine++) {
	    nlohmann::json j_machine = nlohmann::json::object();
	    j_machine["nodeName"] = "none";
	    j_machine["system"] = "linux";
	    j_machine["architecture"] = "x86_64";
	    j_machine["release"] = "none";
	    j_machine["memory"] = 100;
	    j_machine["cpu"] = {{"count", 96}, {"vendor", "none"}, {"speed", 2000}};
	    j_machines.insert(j_machines.end(), j_machine);
	}
	j_workflow["machines"] = j_machines;

	nlohmann::json j_jobs = nlohmann::json::array();
	for (auto itt = tasks.begin(); itt != tasks.end(); ++itt) {
	    nlohmann::json j_job = nlohmann::json::object();
	    j_job["name"] = itt->first;
	    j_job["type"] = "compute";
	    j_job["arguments"] = nlohmann::json::array({"none"});
	    j_job["runtime"] = itt->second->getFlops();
	    j_job["cores"] = 1;
	    j_job["avgCPU"] = itt->second->getAverageCPU();
	    j_job["memory"] = itt->second->getMemoryRequirement()*100;
	    j_job["energy"] = -1;
	    j_job["avgPower"] = -1;
	    j_job["priority"] = 0;
	    j_job["machine"] = "none";
	    
	    nlohmann::json j_parents = nlohmann::json::array();
	    auto parents = itt->second->getParents();
	    for (auto p : parents) {
		j_parents.insert(j_parents.end(), p->getID());
	    }
	    j_job["parents"] = j_parents;

	    nlohmann::json j_files = nlohmann::json::array();
	    auto input_files = itt->second->getInputFiles();
	    double bytes_read = 0;
	    for (auto f: input_files) {
		j_files.insert(j_files.end(), nlohmann::json::object(
			{{"name", f->getID()}, {"size", (long) f->getSize()}, {"link", "input"}}));
		bytes_read += f->getSize();
	    }
	    auto output_files = itt->second->getOutputFiles();
	    double bytes_written = 0;
	    for (auto f: output_files) {
		j_files.insert(j_files.end(), nlohmann::json::object(
			{{"name", f->getID()}, {"size", (long) f->getSize()}, {"link", "output"}}));
		bytes_written += f->getSize();
	    }
	    j_job["files"] = j_files;
	    j_job["bytesRead"] = bytes_read;
	    j_job["bytesWritten"] = bytes_written;

	    j_jobs.insert(j_jobs.end(), j_job);
	}
	j_workflow["jobs"] = j_jobs;

	j["workflow"] = j_workflow;

	/* Write a JSON file for each workflow*/
	std::ofstream file;
	file.exceptions(std::ofstream::failbit | std::ofstream::badbit);
	try {
	    file.open("workflows/" + std::to_string(num_machine) + "_machines/"
	    	                  + itj->first + ".json");
	    file << j.dump(4);
	    file.close();
	} catch (const std::ofstream::failure &e) {
	    throw std::invalid_argument("Cannot open file for output!");
	}
    }

    return 0;

}
