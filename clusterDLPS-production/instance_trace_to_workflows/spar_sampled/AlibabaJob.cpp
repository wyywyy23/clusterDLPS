#include <algorithm>
#include "AlibabaJob.h"
#include "helper/helper.h"

double AlibabaJob::generateFileSize(double exe_time) { // File size in KB
    return exe_time; // Fix with result from data size optimization
}

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

AlibabaJob* AlibabaJob::updateJob(std::string task_name, std::string instance_name, int duration, double avg_cpu, double avg_mem) {

    wrench::WorkflowTask* task = this->addTask(instance_name, duration, 1, 1, avg_mem);
    task->setAverageCPU(avg_cpu);

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

