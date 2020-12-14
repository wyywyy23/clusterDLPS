/**
 * Copyright (c) 2019-2020. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "WorkQueueScheduler.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(WorkQueueScheduler, "Log category for WorkQueue Scheduler Daemon");

namespace wrench {
    namespace workqueue {

        /**
         * @brief Constructor
         */
        WorkQueueScheduler::WorkQueueScheduler() {}


        /**
         * @brief Destructor
         */
        WorkQueueScheduler::~WorkQueueScheduler() {}

        /**
         * @brief Schedule and run a set of pilot jobs on available HTCondor resources
         *
         * @param compute_services: a set of compute services available to run jobs
         *
         * @throw std::runtime_error
         */
        void WorkQueueScheduler::schedulePilotJobs(
                const std::set<std::shared_ptr<wrench::ComputeService>> &compute_services) {

            // TODO: select htcondor service based on condor queue name
            auto htcondor_service = *compute_services.begin();

            auto pilot_job = this->getJobManager()->createPilotJob();
            std::map<std::string, std::string> service_specific_args = {{"-N", "1"},
                                                                        {"-c", "1"},
                                                                        {"-t", "100"}};

            htcondor_service->submitPilotJob(pilot_job, service_specific_args);
        }

        /**
         * @brief
         *
         * @param simulation: a pointer to the simulation object
         */
        void WorkQueueScheduler::setSimulation(Simulation *simulation) {
            this->simulation = simulation;
        }
    }
}
