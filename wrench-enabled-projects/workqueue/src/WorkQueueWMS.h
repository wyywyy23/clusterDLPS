/**
 * Copyright (c) 2019-2020. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef WORKQUEUE_WORKQUEUEWMS_H
#define WORKQUEUE_WORKQUEUEWMS_H

#include <wrench-dev.h>

namespace wrench {
    namespace workqueue {

        /**
         *  @brief An implementation of the WorkQueue meta-scheduler
         */
        class WorkQueueWMS : public WMS {
        public:
            WorkQueueWMS(const std::string &hostname,
                         const std::set<std::shared_ptr<ComputeService>> &htcondor_service,
                         const std::set<std::shared_ptr<StorageService>> &storage_service,
                         std::shared_ptr<FileRegistryService> &file_registry_service);

        protected:
            /***********************/
            /** \cond DEVELOPER    */
            /***********************/

            /***********************/
            /** \endcond           */
            /***********************/

        private:
            int main() override;

            void addJobsToQueue(std::vector<StandardJob *> &jobs);

            void processEventPilotJobStart(std::shared_ptr<PilotJobStartedEvent> event) override;

            void processEventPilotJobExpiration(std::shared_ptr<PilotJobExpiredEvent> event) override;

            void processEventStandardJobCompletion(std::shared_ptr<StandardJobCompletedEvent> event) override;

            void submitJobToPilot(PilotJob *pilot_job);

            /** @brief The job manager */
            std::shared_ptr<JobManager> job_manager;
            /** @brief Whether the workflow execution should be aborted */
            bool abort = false;
            /** **/
            std::deque<StandardJob *> pending_jobs;
            /** **/
            std::map<StandardJob *, PilotJob *> pilot_running_jobs;
        };

    }
}

#endif //WORKQUEUE_WORKQUEUEWMS_H
