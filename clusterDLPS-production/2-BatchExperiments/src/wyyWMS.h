/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef WYY_SIMULATOR_WYYWMS_H
#define WYY_SIMULATOR_WYYWMS_H

#include <wrench-dev.h>

namespace wrench {

    class Simulation;

    /**
     *  @brief A simple WMS implementation
     */
    class wyyWMS : public WMS {

    public:
        wyyWMS(std::unique_ptr<StandardJobScheduler> standard_job_scheduler,
                  std::unique_ptr<PilotJobScheduler> pilot_job_scheduler,
                  const std::set<std::shared_ptr<ComputeService>> &compute_services,
                  const std::set<std::shared_ptr<StorageService>> &storage_services,
		  const std::shared_ptr<FileRegistryService> file_registry_service,
                  const std::string &hostname,
		  const std::map<std::string, std::shared_ptr<StorageService>> &hostname_to_storage_service,
		  const std::string &workflow_file,
		  const double load_factor,
		  const double network_factor,
		  const double slope,
		  const double mean,
		  const double std,
		  const double max);

	Workflow* createWorkflowFromFile(std::string&);

	double truncatedGaussian(double, double, double, std::string);

    protected:

        void processEventStandardJobFailure(std::shared_ptr<StandardJobFailedEvent>) override;

    private:
        int main() override;

        /** @brief The job manager */
        std::shared_ptr<JobManager> job_manager;

        std::map<std::string, std::shared_ptr<StorageService>> hostname_to_storage_service;

	std::string workflow_file;

	double load_factor;
	
	double network_factor;
	double slope;
	double mean;
	double std;
	double max;
        
	/** @brief Whether the workflow execution should be aborted */
        bool abort = false;
    };
}
#endif //WYY_SIMULATOR_WYYWMS_H
