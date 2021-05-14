/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include <iostream>

#include "wyyWMS.h"
#include "helper/endWith.h"

WRENCH_LOG_CATEGORY(wyy_wms, "Log category for wyyWMS");

namespace wrench {

    /**
     * @brief Constructor that creates a wyyWMS with
     *        a scheduler implementation, and a list of compute services
     *
     * @param standard_job_scheduler: a standard job scheduler implementation (if nullptr none is used)
     * @param pilot_job_scheduler: a pilot job scheduler implementation if nullptr none is used)
     * @param compute_services: a set of compute services available to run jobs
     * @param storage_services: a set of storage services available to the WMS
     * @param hostname: the name of the host on which to start the WMS
     */
    wyyWMS::wyyWMS(std::unique_ptr<StandardJobScheduler> standard_job_scheduler,
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
			 const double max) : WMS(
            std::move(standard_job_scheduler),
            std::move(pilot_job_scheduler),
            compute_services,
            storage_services,
            {}, file_registry_service,
            hostname,
            "wyy") {
	this->hostname_to_storage_service = hostname_to_storage_service;
	this->workflow_file = workflow_file;
	this->load_factor = load_factor;
	this->network_factor = network_factor;
	this->slope = slope;
	this->mean = mean;
	this->std = std;
	this->max = max;
	}

    /**
     * @brief main method of wyyWMS daemon
     *
     * @return 0 on completion
     *
     * @throw std::runtime_error
     */
    int wyyWMS::main() {

      TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_GREEN);
      
      // Check whether the WMS has a deferred start time
      checkDeferredStart();
      Workflow* temp_workflow = createWorkflowFromFile(workflow_file);
      if (!temp_workflow) {
	    throw std::runtime_error("Failed to create a workflow from file.");
      }
      WRENCH_DEBUG("Created a workflow from %s, submit time: %f.", workflow_file.c_str(), temp_workflow->getSubmittedTime());

      this->addWorkflow(temp_workflow, temp_workflow->getSubmittedTime());

      for (auto const &f : this->getWorkflow()->getInputFiles()) {
	    try {
		this->simulation->stageFile(f, hostname_to_storage_service[this->getHostname()]);
		WRENCH_DEBUG("Staged input file %s", f->getID().c_str());
	    } catch (std::runtime_error &e) {
		WRENCH_DEBUG("%s", e.what());
		throw std::runtime_error("Cannot stage input file");
	    }
      }

      WRENCH_DEBUG("Starting on host %s", S4U_Simulation::getHostName().c_str());
      WRENCH_DEBUG("About to execute a workflow with %lu tasks", this->getWorkflow()->getNumberOfTasks());

      // Create a job manager
      this->job_manager = this->createJobManager();

      // Create a data movement manager
      std::shared_ptr<DataMovementManager> data_movement_manager = this->createDataMovementManager();

      // Start bandwidth meters
      // std::shared_ptr<BandwidthMeterService> bw_meter_service = createBandwidthMeter(this->simulation->getLinknameList(), 0.01);

      // Perform static optimizations
      runStaticOptimizations();

      while (true) {

        // Get the ready tasks
        std::vector<WorkflowTask *> ready_tasks = this->getWorkflow()->getReadyTasks();

        // Get the available compute services
        auto compute_services = this->getAvailableComputeServices<ComputeService>();

        if (compute_services.empty()) {
          WRENCH_DEBUG("Aborting - No compute services available!");
          break;
        }

        // Submit pilot jobs
        if (this->getPilotJobScheduler()) {
          WRENCH_DEBUG("Scheduling pilot jobs...");
          this->getPilotJobScheduler()->schedulePilotJobs(this->getAvailableComputeServices<ComputeService>());
        }

        // Perform dynamic optimizations
        runDynamicOptimizations();

        // Run ready tasks with defined scheduler implementation
        WRENCH_DEBUG("Scheduling tasks...");
	for (auto task: ready_tasks) {
	    for (auto f : task->getInputFiles()) {
                std::string local_host = "";
                if (f->isOutput()){
                    local_host = f->getOutputOf()->getExecutionHost();
	    	    if (not local_host.empty()){
		        if (not hostname_to_storage_service[local_host]->lookupFile(f, FileLocation::LOCATION(hostname_to_storage_service[local_host]))) {
            	        data_movement_manager->doSynchronousFileCopy(f,
		            FileLocation::LOCATION(hostname_to_storage_service["master"]),
		            FileLocation::LOCATION(hostname_to_storage_service[local_host]));
		        }
	    	    } else {
            	        data_movement_manager->doSynchronousFileCopy(f,
		            FileLocation::LOCATION(hostname_to_storage_service["master"]),
		            FileLocation::LOCATION(hostname_to_storage_service[this->getHostname()]));
		    }
		}
	    }
	}
        this->getStandardJobScheduler()->scheduleTasks(this->getAvailableComputeServices<ComputeService>(), ready_tasks);

        // Wait for a workflow execution event, and process it
        try {
          this->waitForAndProcessNextEvent();
        } catch (WorkflowExecutionException &e) {
          WRENCH_DEBUG("Error while getting next execution event (%s)... ignoring and trying again",
                      (e.getCause()->toString().c_str()));
          continue;
        }
        if (this->abort || this->getWorkflow()->isDone()) {
          break;
        }
      }

      // S4U_Simulation::sleep(10);

      WRENCH_DEBUG("--------------------------------------------------------");
      if (this->getWorkflow()->isDone()) {
        WRENCH_DEBUG("Workflow execution is complete!");
      } else {
        WRENCH_DEBUG("Workflow execution is incomplete!");
      }

      WRENCH_DEBUG("wyyWMS Daemon started on host %s terminating", S4U_Simulation::getHostName().c_str());

      for (auto &t : this->getWorkflow()->getTasks()) {
	WRENCH_INFO("%s,%s,%s,%ld,%ld,%f,%f,%f,%f,%f,%f,%f,%f\n",
		this->getWorkflow()->getName().c_str(),
		t->getID().c_str(),
		t->getExecutionHost().c_str(),
		simulation->getHostNumCores(t->getExecutionHost()),
		t->getNumCoresAllocated(),
		t->getReadInputStartDate(),
		t->getReadInputEndDate(),
		t->getComputationStartDate(),
		t->getComputationEndDate(),
		t->getWriteOutputStartDate(),
		t->getWriteOutputEndDate(),
		t->getStaticStartTime(),
		t->getStaticEndTime()
	);
	this->getWorkflow()->removeTask(t);
//	t->deleteTask();
      }
      for (auto &f : this->getWorkflow()->getFiles()) {
	this->getWorkflow()->removeFile(f);
//	f->deleteFile();
      }
      this->getWorkflow()->deleteWorkflow();
      this->job_manager.reset();

      return 0;
    }

    /**
     * @brief Process a WorkflowExecutionEvent::STANDARD_JOB_FAILURE
     *
     * @param event: a workflow execution event
     */
    void wyyWMS::processEventStandardJobFailure(std::shared_ptr<StandardJobFailedEvent> event) {
      auto job = event->standard_job;
      WRENCH_DEBUG("Notified that a standard job has failed (all its tasks are back in the ready state)");
      WRENCH_DEBUG("CauseType: %s", event->failure_cause->toString().c_str());
      WRENCH_DEBUG("As wyyWMS, I abort as soon as there is a failure");
      this->abort = true;
    }

    Workflow* wyyWMS::createWorkflowFromFile(std::string& workflow_file) {

      Workflow* workflow = nullptr;

      if (endWith(workflow_file, "json")) {
	try {
	    workflow = PegasusWorkflowParser::createWorkflowFromJSON(workflow_file, "1f", load_factor);
	} catch (std::invalid_argument &e) {
	  std::cerr << "Cannot create a workflow from " << workflow_file << ": " << e.what() << std::endl;
	}
      } else if (endWith(workflow_file, "dax")) {
	try {
	  workflow = PegasusWorkflowParser::createWorkflowFromDAX(workflow_file, "1f");
	} catch (std::invalid_argument &e) {
	    std::cerr << "Cannot create a workflow from " << workflow_file << ": " << e.what() << std::endl;
	}

      } else {
	std::cerr << "Cannot create a workflow from " << workflow_file << ": not supporting formats other than .json and .dax" << std::endl;
      }

      // update workflow by network factor

      for (auto file : workflow->getFiles()) {
	double size = truncatedGaussian(mean, std, max, workflow->getName());
	if (file->isOutput()) {
	  size += file->getOutputOf()->getFlops() * slope;
	}
	file->setSize(size * network_factor / (1.0 + network_factor));
      }

      for (auto task : workflow->getTasks()) {
	task->setFlops(task->getFlops() / (1.0 + network_factor));
      }

      return workflow;
    }

    double wyyWMS::truncatedGaussian(double mean, double std, double max, std::string name) {
	std::mt19937 rng;
	std::normal_distribution<double> dist(mean, std);
	std::seed_seq seed(name.begin(), name.end());
	rng.seed(seed);
	return std::min(std::max(dist(rng), 0.0), max);
    }
}


