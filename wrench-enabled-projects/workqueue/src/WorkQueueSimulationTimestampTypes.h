/**
 * Copyright (c) 2019-2020. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef WORKQUEUE_SIMULATIONTIMESTAMPTYPES_H
#define WORKQUEUE_SIMULATIONTIMESTAMPTYPES_H

#include <wrench.h>

namespace wrench {

    class WorkflowTask;

    namespace workqueue {

        class SimulationTimestampJobSubmitted {
        public:
            SimulationTimestampJobSubmitted(WorkflowTask *task);

            WorkflowTask *getTask();

            double getClock();

        private:
            double clock;
            WorkflowTask *task;
        };

        class SimulationTimestampJobScheduled {

        public:
            SimulationTimestampJobScheduled(WorkflowTask *task);

            WorkflowTask *getTask();

            double getClock();

        private:
            double clock;
            WorkflowTask *task;
        };

        class SimulationTimestampJobCompletion {

        public:
            SimulationTimestampJobCompletion(WorkflowTask *task);

            WorkflowTask *getTask();

            double getClock();

        private:
            double clock;
            WorkflowTask *task;
        };
    }
}

#endif //WORKQUEUE_SIMULATIONTIMESTAMPTYPES_H
