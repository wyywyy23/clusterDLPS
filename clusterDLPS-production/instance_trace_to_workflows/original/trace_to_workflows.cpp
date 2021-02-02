#include <iostream>
#include <iomanip>
#include <vector>
#include <list>
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

    int start_hour = (job->getSubmittedTime() - 1) / 3600;

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
//	auto child_tasks = file->getInputOf();
//	if (not child_tasks.empty()) {
//	    double avg_flops = 0;
//	    for (auto itt = child_tasks.begin(); itt != child_tasks.end(); ++itt) {
//		avg_flops += itt->second->getFlops();
//	    }
//	    avg_flops = avg_flops / child_tasks.size();
//	    file->setSize(job->generateFileSize(avg_flops));
//	}
	file->setSize(job->generateFileSize(job->getSubmittedTime()));
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
    j_workflow["executedAt"] = job->getSubmittedTime() - start_hour * 3600;
    /* nlohmann::json j_machines = nlohmann::json::array();
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
    j_workflow["machines"] = j_machines; */

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
	j_job["endTimeInTrace"] = itt->second->getStaticEndTime() - start_hour * 3600;
	j_job["startTimeInTrace"] = itt->second->getStaticStartTime() - start_hour * 3600;	   
 
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
	file.open(output_path + std::to_string(start_hour) + "-" + std::to_string(start_hour + 1) + "/" + job->getName() + ".json");
	file << j.dump(4);
	file.close();
    } catch (const std::ofstream::failure &e) {
	throw std::invalid_argument("Cannot open file for output!");
    }

    return 1;
}

int main(int argc, char **argv) {

    if (argc != 5) {
	std::cerr << "Usage: " << argv[0] << "<start time offset (hrs)> <duration (hrs)> <time out (s)> <dump interval>" << std::endl;
	exit(1);
    }
    
    const int max_num_machine = 4096;
    int start_time_offset = std::atoi(argv[1]);
    int trace_duration = std::atoi(argv[2]);
    double time_out = std::atof(argv[3]);
    int dump_interval = std::atoi(argv[4]);

    std::string trace_file_path = "trace/batch_instance.csv";
    std::string output_path = "output/workflows/";

    std::cerr << "Trace file:\t" << trace_file_path << std::endl;
    std::cerr << "Output Path:\t" << output_path << std::endl;

    /* Add another big offset to start time */
//    start_time_offset = start_time_offset + 48;

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
    std::list<std::string> job_list;
    std::map<std::string, bool> job_out_of_range;
//    std::list<std::string> job_out_of_range_list;

    long lines_read = 0;
    long num_dumped_jobs = 0;
    while (task_trace.read_row(instance_name, task_name, job_name, task_type, status, start_time, end_time, machine_id, sequence_number, total_sequence_number, avg_cpu, max_cpu, avg_mem, max_mem)) {

	lines_read++;
	bool verbose = lines_read % 10000 == 0 ? true : false;
	if (verbose) {
	    std::cerr << "Read " << std::setw(10) << (double) lines_read / 1351255775 * 100 << "\% of file...\t"
		  << "# of queued jobs: " << std::setw(6) << jobs.size() << "\t"
		  << "# of dumped jobs: " << std::setw(8) << num_dumped_jobs <<"\r";
	}
	// end_time = max(end_time, start_time + 1);

	/* Fix identical instance id by sequence number */
	instance_name = instance_name + "_" + std::to_string(sequence_number);

	/* Check start time in range */
	if (job_out_of_range[job_name] == true) continue;
	if (start_time <= start_time_offset * 3600 
		or start_time > (start_time_offset + trace_duration) * 3600) {
	    job_out_of_range[job_name] = true;
//	    job_out_of_range_list.push_back(job_name);
	    if (jobs.find(job_name) != jobs.end()) {
		delete jobs[job_name];
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
//	    job_out_of_range_list.push_back(job_name);
	    if (jobs.find(job_name) != jobs.end()) {
		delete jobs[job_name];
                jobs.erase(job_name);
            }
            continue;
	}

	/* Skip failed task */
	if (status != "Terminated") {
	    job_out_of_range[job_name] = true;
//	    job_out_of_range_list.push_back(job_name);
	    if (jobs.find(job_name) != jobs.end()) {
		delete jobs[job_name];
                jobs.erase(job_name);
            }
	    continue;
	}

//	/* Limit the size of out-of-range job list */
//	while (job_out_of_range.size() > 100000) {
//	    job_out_of_range.erase(job_out_of_range_list.front());
//	    job_out_of_range_list.pop_front();
//	}

	start_time = start_time - start_time_offset * 3600;
	end_time = end_time - start_time_offset * 3600;
	if (jobs.empty() || jobs.find(job_name) == jobs.end()) { /* a new job */
	    AlibabaJob* job = new AlibabaJob();
	    job->setName(job_name);
	    job->setSubmittedTime(start_time);
	    jobs[job_name] = job->updateJob(task_name, instance_name, start_time, end_time, avg_cpu, avg_mem, host_id);
	    job_list.push_back(job_name);

	    /* dump oldest job */
	    if (jobs.size() > dump_interval) {
		while (jobs.find(job_list.front()) == jobs.end()) { // Delete invalid jobs
		    job_list.pop_front();
		}
		std::string job_to_dump = job_list.front();
		int is_dumped = dumpJob(jobs[job_to_dump], output_path, time_out);
		delete jobs[job_to_dump];
		jobs.erase(job_to_dump);
		job_list.pop_front();
		num_dumped_jobs += is_dumped;
		job_out_of_range[job_to_dump] = true;
//		job_out_of_range_list.push_back(job_to_dump);
	    }

	} else { /* existing job */
	    jobs[job_name] = jobs[job_name]->updateJob(task_name, instance_name, start_time, end_time, avg_cpu, avg_mem, host_id);
	}

	if (verbose) {
	    std::cerr << "Read " << std::setw(10) << (double) lines_read / 1351255775 * 100 << "\% of file...\t"
		  << "# of queued jobs: " << std::setw(6) << jobs.size() << "\t"
		  << "# of dumped jobs: " << std::setw(8) << num_dumped_jobs <<"\r";
	}
    }
    
    /* dump remaining jobs */
    for (auto itj = jobs.begin(); itj != jobs.end(); ++itj) {
	int is_dumped = dumpJob(itj->second, output_path, time_out);
	delete itj->second;
	jobs.erase(itj->first);
	num_dumped_jobs += is_dumped;

	std::cerr << "Read " << std::setw(10) << (double) lines_read / 1351255775 * 100 << "\% of file...\t"
		  << "# of queued jobs: " << std::setw(6) << jobs.size() << "\t"
		  << "# of dumped jobs: " << std::setw(8) << num_dumped_jobs <<"\r";

    }
    std::cerr << std::endl;

    return 0;
}
