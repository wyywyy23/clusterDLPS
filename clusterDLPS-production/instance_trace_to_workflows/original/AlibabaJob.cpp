#include <algorithm>
#include "AlibabaJob.h"
#include "helper/helper.h"

/*double AlibabaJob::generateFileSize(std::string job_id, std::string file_id) { // File size in KB
    std::mt19937 rng1;
    std::mt19937 rng2;
    std::seed_seq seed1 (job_id.begin(), job_id.end());
    std::seed_seq seed2 (file_id.begin(), file_id.end());
    std::normal_distribution<double> dist1(9000000, 2110000000);
    std::normal_distribution<double> dist2(0, 37500);
    rng1.seed(seed1);
    rng2.seed(seed2);
    double file_size = max(dist1(rng1) + dist2(rng2), 0.0);

    return file_size; // Fix here with data size optimization result
}*/

void AlibabaJob::addControlDependency(wrench::WorkflowTask* src, wrench::WorkflowTask* dst, bool redundant_dependencies) {

    if ((src == nullptr) || (dst == nullptr)) {
        throw std::invalid_argument("Workflow::addControlDependency(): Invalid arguments");
    }

    if (redundant_dependencies || not this->dag.doesPathExist(src, dst)) {

        this->dag.addEdge(src, dst);

        // dst->updateTopLevel();

        /* if (src->getState() != wrench::WorkflowTask::State::COMPLETED) {
            dst->setInternalState(wrench::WorkflowTask::InternalState::TASK_NOT_READY);
            dst->setState(wrench::WorkflowTask::State::NOT_READY);
        } */
    }
}

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

AlibabaJob* AlibabaJob::updateJob(std::string task_name, std::string instance_name, double start_time, double end_time, double avg_cpu, double avg_mem, long host_id) {

    wrench::WorkflowTask* task = this->addTask(instance_name, max(end_time - start_time, 0.0), 1, 1, avg_mem);
    task->setAverageCPU(avg_cpu);

    /* Assign static host */
    task->setStaticHost(host_id);

    /* Static end time */
    task->setStaticEndTime(end_time);
    task->setStaticStartTime(start_time);

    /* Update job start time */
    if (start_time < this->getSubmittedTime()) {
	this->setSubmittedTime(start_time);
    }

    /* Remove first letter and split into individual task IDs */
    std::vector<std::string> split_names;
    if (task_name.length() < 4 || (task_name.length() >= 4 && task_name.substr(0, 4).compare("task") != 0)) {
        split_names = splitString(task_name.erase(0,1), "_");
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

