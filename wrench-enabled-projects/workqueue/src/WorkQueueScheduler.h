/**
 * Copyright (c) 2019-2020. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef WORKQUEUE_WORKQUEUESCHEDULER_H
#define WORKQUEUE_WORKQUEUESCHEDULER_H

#include <wrench-dev.h>

namespace wrench {

    class Simulation;

    namespace workqueue {

        /**
         * @brief WorkQueue scheduler daemon
         */
        class WorkQueueScheduler : public PilotJobScheduler {
        public:
            WorkQueueScheduler();

            ~WorkQueueScheduler();

            /***********************/
            /** \cond DEVELOPER    */
            /***********************/

            void schedulePilotJobs(const std::set<std::shared_ptr<ComputeService>> &compute_services) override;

            void setSimulation(Simulation *simulation);

            /***********************/
            /** \endcond           */
            /***********************/

        protected:
            /** @brief */
            Simulation *simulation;
        };

    }
}

#endif //WORKQUEUE_WORKQUEUESCHEDULER_H
