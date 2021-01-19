#include <iostream>
#include <iomanip>
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

int main(int argc, char **argv) {

    if (argc != 4) {
	std::cerr << "Usage: " << argv[0] << "<start time offset (hrs)> <duration (hrs)> <time out (s)>" << std::endl;
	exit(1);
    }
    
    const int max_num_machine = 4096;
    int start_time_offset = std::atoi(argv[1]);
    int trace_duration = std::atoi(argv[2]);
    double time_out = std::atof(argv[3]);

    std::string trace_file_path = "trace/batch_instance.csv";
    std::string output_path = "output/workflows/";

    std::cerr << "Trace file:\t" << trace_file_path << std::endl;
    std::cerr << "Output Path:\t" << output_path << std::endl;

    /* Add another big offset to start time */
//    start_time_offset = start_time_offset + 48;

    time_t rawtime;
    struct tm* timeinfo;

    const int num_instance_column = 14;

    /* Open instance trace */
    io::CSVReader<num_instance_column> task_trace(trace_file_path);
    
    std::string instance_name;
    std::string task_name;
    std::string job_name;
    std::string task_type;
    std::string status;
    long start_time;
    long end_time;
    std::string machine_id;
    long sequence_number;
    long total_sequence_number;
    double avg_cpu;
    double max_cpu;
    double avg_mem;
    double max_mem;

    /* Initiate a map of workflows (jobs) as <jobID, workflow>*/
    std::map<std::string, AlibabaJob*> jobs;
    std::map<std::string, bool> job_out_of_range;

    long lines_read = 0;
    while (task_trace.read_row(instance_name, task_name, job_name, task_type, status, start_time, end_time, machine_id, sequence_number, total_sequence_number, avg_cpu, max_cpu, avg_mem, max_mem)) {

	
	std::cerr << "Read " << std::setw(10) << (double) ++lines_read / 1351255775 *100 << "\% of file...\t" << "# of valid jobs: " << jobs.size() << "\r";

	/* Fix identical instance id by sequence number */
	instance_name = instance_name + "_" + std::to_string(sequence_number);

	/* Check start time in range */
	if (job_out_of_range[job_name] == true) continue;
	if (start_time <= start_time_offset * 3600 
		or start_time > (start_time_offset + trace_duration) * 3600) {
	    job_out_of_range[job_name] = true;
	    if (jobs.find(job_name) != jobs.end()) {
		jobs.erase(job_name);
	    }
	    continue;
	}

    	/* Get (long) static host id from machine_id */
    	std::vector<std::string> machine_id_split = splitString(machine_id, "_");
    	long host_id = std::stol(machine_id_split.back()) - 1;

	/* Check machine in range */
	if (host_id >= max_num_machine) {
	    job_out_of_range[job_name] = true;
	    if (jobs.find(job_name) != jobs.end()) {
                jobs.erase(job_name);
            }
            continue;
	}

	/* Skip failed task */
	if (status == "Failed") {
	    continue;
	}

	std::cerr << "Read " << std::setw(10) << (double) lines_read / 1351255775 *100 << "\% of file...\t" << "# of valid jobs: " << jobs.size() << "\r";

	start_time = start_time - start_time_offset * 3600;
	end_time = end_time - start_time_offset * 3600;
	if (jobs.empty() || jobs.find(job_name) == jobs.end()) { /* a new job */
	    AlibabaJob* job = new AlibabaJob();
	    job->setName(job_name);
	    job->setSubmittedTime(start_time);
	    jobs[job_name] = job->updateJob(task_name, instance_name, start_time, end_time, avg_cpu, avg_mem, host_id);
	} else { /* existing job */
	    jobs[job_name] = jobs[job_name]->updateJob(task_name, instance_name, start_time, end_time, avg_cpu, avg_mem, host_id);
	}
    }
    
    long num_skipped = 0;
    for (auto itj = jobs.begin(); itj != jobs.end(); ++itj) {
	cerr << "Generating JSON for " <<itj->first << "..." << std::endl;
	double this_time_out = time_out;

	/* Add an output file to each task instance */
	auto tasks = itj->second->getTaskMap();
	for (auto itt = tasks.begin(); itt != tasks.end(); ++itt) {
	    wrench::WorkflowFile* output_file = nullptr;
	    std::string file_name = itt->first + "_output";
	    try {
		output_file = itj->second->getFileByID(file_name);
		std::cerr << "Got duplicate file!" << std::endl;
	    } catch (const std::invalid_argument &ia) {
	        output_file = itj->second->addFile(file_name, itj->second->generateFileSize(itt->second->getFlops()));
		itt->second->addOutputFileWithoutDependencies(output_file);
	    }
	}

        /* Add task dependencies according to the dependency map */
	auto d_map = itj->second->getDependencyMap();
	auto t_map = itj->second->getTaskInstanceMap();
	auto itd = d_map.begin();
	while (itd != d_map.end() && this_time_out > 0) {
	    auto child_range = t_map.equal_range(itd->first);
	    auto parent_range = t_map.equal_range(itd->second);
	    auto itc = child_range.first;
	    while (itc != child_range.second && this_time_out > 0) {
		auto itp = parent_range.first;
		while (itp != parent_range.second && this_time_out > 0) {
		    clock_t start_time = clock();
		    tasks[itc->second]->addInputFileWithoutDependencies(itj->second->getFileByID(itp->second + "_output"));
		    itj->second->addControlDependency(tasks[itp->second], tasks[itc->second]);
	    	    this_time_out -= (double) (clock() - start_time)/CLOCKS_PER_SEC;
		    itp++;
		}
		itc++;
	    }
	    itd++;
	}
	if (this_time_out <= 0 ) {
	    std::cerr << "Time out! Skipped." << std::endl;
	    num_skipped++;
	    continue;
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
		input_file = itj->second->addFile(file_name, itj->second->generateFileSize(ite->second->getFlops()));
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
	for (int idx_machine = 0; idx_machine < max_num_machine; idx_machine++) {
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
	    j_job["machine"] = std::to_string(itt->second->getStaticHost());
	    j_job["endTimeInTrace"] = itt->second->getStaticEndTime();
	    
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
	int start_hour = (itj->second->getSubmittedTime() - 1) / 3600;
	std::ofstream file;
	file.exceptions(std::ofstream::failbit | std::ofstream::badbit);
	try {
	    file.open(output_path + std::to_string(start_hour) + "-" + std::to_string(start_hour + 1) + "/" + itj->first + ".json");
	    file << j.dump(4);
	    file.close();
	} catch (const std::ofstream::failure &e) {
	    throw std::invalid_argument("Cannot open file for output!");
	}
    }

    std::cerr << "Skipped " << std::to_string(num_skipped) << "/" << std::to_string(jobs.size()) << " = " << std::to_string((double) num_skipped/jobs.size()*100) << "% jobs." << std::endl;
    return 0;

}
