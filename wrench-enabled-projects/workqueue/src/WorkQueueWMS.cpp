/**
 * Copyright (c) 2019-2020. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "WorkQueueWMS.h"
#include "WorkQueueScheduler.h"
#include "WorkQueueSimulationTimestampTypes.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(WorkQueueWMS, "Log category for WorkQueue WMS");

namespace wrench {
    namespace workqueue {

        /**
         * @brief Create a WorkQueue WMS with a workflow instance, a list of compute services
         *
         * @param hostname: the name of the host on which to start the WMS
         * @param htcondor_service: an HTCondor service available to run jobs
         * @param storage_service: a storage service available to the WMS
         * @param file_registry_service:
         */
        WorkQueueWMS::WorkQueueWMS(const std::string &hostname,
                                   const std::set<std::shared_ptr<wrench::ComputeService>> &compute_services,
                                   const std::set<std::shared_ptr<wrench::StorageService>> &storage_services,
                                   std::shared_ptr<FileRegistryService> &file_registry_service) :
                WMS(nullptr, std::unique_ptr<PilotJobScheduler>(new WorkQueueScheduler()),
                    compute_services, storage_services, {},
                    file_registry_service, hostname, "workqueue") {}

        /**
         * @brief main method of the WorkQueue WMS daemon
         *
         * @return 0 on completion
         *
         * @throw std::runtime_error
         */
        int WorkQueueWMS::main() {
            TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_GREEN);

            // Check whether there is a deferred start time
            checkDeferredStart();

            WRENCH_INFO("Starting WorkQueue on host %s listening on mailbox_name %s",
                        S4U_Simulation::getHostName().c_str(),
                        this->mailbox_name.c_str());WRENCH_INFO(
                    "WorkQueue is about to execute a workflow with %lu tasks",
                    this->getWorkflow()->getNumberOfTasks());

            auto default_compute_service = *this->getAvailableComputeServices<HTCondorComputeService>().begin();
            auto default_storage_service = default_compute_service->getLocalStorageService();

            // Create a job manager
            this->job_manager = this->createJobManager();
            auto data_movement_manager = this->createDataMovementManager();

            // scheduler
            auto pilot_job_scheduler = (WorkQueueScheduler *) this->getPilotJobScheduler();
            pilot_job_scheduler->setSimulation(this->simulation);
            pilot_job_scheduler->setDataMovementManager(data_movement_manager);

            while (true) {
                auto ready_tasks = this->getWorkflow()->getReadyTasks();
                std::vector<StandardJob *> ready_jobs;

                int count = 0;
                for (auto task : ready_tasks) {
                    std::map<WorkflowFile *, std::shared_ptr<FileLocation>> file_locations;
                    for (auto f : task->getInputFiles()) {
                        file_locations[f] = FileLocation::LOCATION(default_storage_service);
                    }
                    for (auto f : task->getOutputFiles()) {
                        file_locations[f] = FileLocation::LOCATION(default_storage_service);
                    }

                    ready_jobs.push_back(this->job_manager->createStandardJob(task, file_locations));
                }

                if (not ready_jobs.empty()) {
                    this->addJobsToQueue(ready_jobs);
                    count = 0;
                    while (count < ready_jobs.size() && count < 33) {
                        pilot_job_scheduler->schedulePilotJobs(this->getAvailableComputeServices<ComputeService>());
                        count++;
                    }
                }

                // Wait for a workflow execution event, and process it
                try {
                    this->waitForAndProcessNextEvent();
                } catch (WorkflowExecutionException &e) { WRENCH_INFO(
                            "Error while getting next execution event (%s)... ignoring and trying again",
                            (e.getCause()->toString().c_str()));
                    continue;
                }

                if (this->abort || this->getWorkflow()->isDone()) {
                    break;
                }
            }

            WRENCH_INFO("--------------------------------------------------------");
            if (this->getWorkflow()->isDone()) { WRENCH_INFO("Workflow execution is complete!");
            } else { WRENCH_INFO("Workflow execution is incomplete!");
            }

            WRENCH_INFO("WorkQueue Daemon started on host %s terminating", S4U_Simulation::getHostName().c_str());

            this->job_manager.reset();

            return 0;
        }

        /**
         * @brief
         *
         * @param tasks
         */
        void WorkQueueWMS::addJobsToQueue(std::vector<StandardJob *> &jobs) {
            for (auto job : jobs) {
                for (auto task : job->getTasks()) {
                    // create job submitted event
                    this->simulation->getOutput().addTimestamp<SimulationTimestampJobSubmitted>(
                            new SimulationTimestampJobSubmitted(task));
                    task->setState(WorkflowTask::PENDING);
                }
                this->pending_jobs.push_back(job);
            }
        }

        /**
         * @brief
         *
         * @param event:
         */
        void WorkQueueWMS::processEventPilotJobStart(std::shared_ptr<wrench::PilotJobStartedEvent> event) {
            WRENCH_INFO("A pilot job has started: %s", event->pilot_job->getName().c_str());
            this->submitJobToPilot(event->pilot_job);
        }

        /**
         * @brief
         *
         * @param event:
         */
        void WorkQueueWMS::processEventPilotJobExpiration(std::shared_ptr<wrench::PilotJobExpiredEvent> event) {
            WRENCH_INFO("A pilot job has expired: %s", event->pilot_job->getName().c_str());
            for (auto entry : this->pilot_running_jobs) {
                if (entry.second == event->pilot_job) {
                    this->pending_jobs.push_front(entry.first);
                    this->pilot_running_jobs.erase(this->pilot_running_jobs.find(entry.first));
                    break;
                }
            }
        }

        /**
         * @brief
         *
         * @param event:
         */
        void WorkQueueWMS::processEventStandardJobCompletion(std::shared_ptr<wrench::StandardJobCompletedEvent> event) {
            auto job = event->standard_job;WRENCH_INFO("A standard job has completed job %s", job->getName().c_str());
            std::string callback_mailbox = job->popCallbackMailbox();
            for (auto task : job->getTasks()) { WRENCH_INFO("    Task completed: %s (%s)", task->getID().c_str(),
                                                            callback_mailbox.c_str());
                // create job completion event
                this->simulation->getOutput().addTimestamp<SimulationTimestampJobCompletion>(
                        new SimulationTimestampJobCompletion(task));
            }
            auto job_it = this->pilot_running_jobs.find(job);
            auto pilot_job = job_it->second;
            this->pilot_running_jobs.erase(job_it);

            // submit additional jobs if in queue
            this->submitJobToPilot(pilot_job);
        }

        /**
         * @brief
         *
         * @param pilot_job:
         */
        void WorkQueueWMS::submitJobToPilot(wrench::PilotJob *pilot_job) {
            if (not this->pending_jobs.empty()) {
                auto job = this->pending_jobs.front();

                std::map<std::string, std::string> specific_args;
                job->pushCallbackMailbox(this->job_manager->mailbox_name);
                pilot_job->getComputeService()->submitStandardJob(job, specific_args);
                this->pilot_running_jobs.insert(std::make_pair(job, pilot_job));

                this->pending_jobs.pop_front();WRENCH_INFO("Submitting %s to %s", job->getName().c_str(),
                                                           pilot_job->getName().c_str());
                for (auto task : job->getTasks()) { WRENCH_INFO("  Task submitted: %s", task->getID().c_str());
                    // create job scheduled event
                    this->simulation->getOutput().addTimestamp<SimulationTimestampJobScheduled>(
                            new SimulationTimestampJobScheduled(task));
                }WRENCH_INFO("There are still %lu pending jobs", this->pending_jobs.size());
            }
        }
    }
}
