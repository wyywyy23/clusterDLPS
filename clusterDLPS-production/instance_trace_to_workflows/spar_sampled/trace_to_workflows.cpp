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
#include "helper/helper.h"

#include "AlibabaJob.h"

int dumpJob(AlibabaJob* job, std::string output_path, double time_out) {
    double this_time_out = time_out;
    time_t rawtime;
    struct tm* timeinfo;

    /* Add an output file to each task instance */
    auto tasks = job->getTaskMap();
    for (auto itt = tasks.begin(); itt != tasks.end(); ++itt) {
	wrench::WorkflowFile* output_file = nullptr;
	std::string file_name = itt->first + "_output";
	try {
	    output_file = job->getFileByID(file_name);
	} catch (const std::invalid_argument &ia) {
	    output_file = job->addFile(file_name, 0);
	    itt->second->addOutputFileWithoutDependencies(output_file);
	}
    }

    /* Add task dependencies according to the dependency map */
    auto d_map = job->getDependencyMap();
    auto t_map = job->getTaskInstanceMap();
    auto itd = d_map.begin();
    while (itd != d_map.end() && this_time_out > 0) {
	auto child_range = t_map.equal_range(itd->first);
	auto parent_range = t_map.equal_range(itd->second);
	auto itc = child_range.first;
	while (itc != child_range.second && this_time_out > 0) {
	    auto itp = parent_range.first;
	    while (itp != parent_range.second && this_time_out > 0) {
		clock_t time_out_start = clock();
		tasks[itc->second]->addInputFileWithoutDependencies(job->getFileByID(itp->second + "_output"));
		job->addControlDependency(tasks[itp->second], tasks[itc->second]);
	    	this_time_out -= (double) (clock() - time_out_start)/CLOCKS_PER_SEC;
		itp++;
	    }
	    itc++;
	}
	itd++;
    }
    if (this_time_out <= 0 ) {
	return 0;
    }

    /* Add an input file to each root task */
    auto entry_tasks = job->getEntryTaskMap();
    for (auto ite = entry_tasks.begin(); ite != entry_tasks.end(); ++ite) {
	wrench::WorkflowFile* input_file = nullptr;
	std::string file_name = ite->first + "_input";
	try {
	    input_file = job->getFileByID(file_name);
	} catch (const std::invalid_argument &ia) {
	    input_file = job->addFile(file_name, 0);
	    ite->second->addInputFileWithoutDependencies(input_file);
	}
    }

    /* Update file size according to task execution time */
    auto all_files = job->getFiles();
    for (auto file : all_files ) {
	auto child_tasks = file->getInputOf();
	if (not child_tasks.empty()) {
	    double avg_flops = 0;
	    for (auto itt = child_tasks.begin(); itt != child_tasks.end(); ++itt) {
		avg_flops += itt->second->getFlops();
	    }
	    avg_flops = avg_flops / child_tasks.size();
	    file->setSize(job->generateFileSize(avg_flops));
	}
    }


    /* Create JSON structure*/
    nlohmann::json j;
    j["name"] = job->getName();
    j["description"] = "This job contains " + std::to_string(job->getNumberOfTasks()) + " tasks.";
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
    j_workflow["executedAt"] = job->getSubmittedTime();
 /* nlohmann::json j_machines = nlohmann::json::array();
 *     for (int idx_machine = 0; idx_machine < max_num_machine; idx_machine++) {
 *         nlohmann::json j_machine = nlohmann::json::object();
 *         j_machine["nodeName"] = "none";
 *     	   j_machine["system"] = "linux";
 *     	   j_machine["architecture"] = "x86_64";
 *     	   j_machine["release"] = "none";
 *         j_machine["memory"] = 100;
 *     	   j_machine["cpu"] = {{"count", 96}, {"vendor", "none"}, {"speed", 2000}};
 *     	   j_machines.insert(j_machines.end(), j_machine);
 *     }
 *     j_workflow["machines"] = j_machines; */

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
	j_job["machine"] = "-1";
 
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
	file.open(output_path + job->getName() + ".json");
	file << j.dump(4);
	file.close();
    } catch (const std::ofstream::failure &e) {
	throw std::invalid_argument("Cannot open file for output!");
    }

    return 1;
}

int main(int argc, char **argv) {

    if (argc != 4) {
	std::cerr << "Usage: " << argv[0] << "<number of machines> <time out in seconds>" << std::endl;
	exit(1);
    }
    
    int num_machine = 4;
    double time_out = std::atof(argv[2]);
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

    std::string partial_path = argv[3];

    std::string output_path = partial_path + std::to_string(num_machine) + "_machines/";

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
    
    long num_skipped = 0;
    for (auto itj = jobs.begin(); itj != jobs.end(); ++itj) {
	int is_dumped = dumpJob(itj->second, output_path, time_out);
        if (not is_dumped) { num_skipped++; }
    }

    std::cerr << "Skipped " << std::to_string(num_skipped) << "/" << std::to_string(jobs.size()) << " = " << std::to_string((double) num_skipped/jobs.size()*100) << "% jobs." << std::endl;
    return 0;

}
