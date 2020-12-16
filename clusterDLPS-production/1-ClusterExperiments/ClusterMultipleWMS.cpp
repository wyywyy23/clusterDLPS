/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include <iostream>
#include <wrench.h>
#include "helper/getAllFilesInDir.h"

#include "SimpleWMS.h"
#include "scheduler/CloudStandardJobScheduler.h"
#include <wrench/tools/pegasus/PegasusWorkflowParser.h>


static bool ends_with(const std::string& str, const std::string& suffix) {
    return str.size() >= suffix.size() && 0 == str.compare(str.size()-suffix.size(), suffix.size(), suffix);
}



/**
 * @brief An example that demonstrate how to run a simulation of a simple Workflow
 *        Management System (WMS) (implemented in SimpleWMS.[cpp|h]).
 *
 * @param argc: argument count
 * @param argv: argument array
 * @return 0 if the simulation has successfully completed
 */
int main(int argc, char **argv) {



    /*
     * Declaration of the top-level WRENCH simulation object
     */
    wrench::Simulation simulation;

    /*
     * Initialization of the simulation, which may entail extracting WRENCH-specific and
     * Simgrid-specific command-line arguments that can modify general simulation behavior.
     * Two special command-line arguments are --help-wrench and --help-simgrid, which print
     * details about available command-line arguments.
     */
    simulation.init(&argc, argv);

    /*
     * Parsing of the command-line arguments for this WRENCH simulation
     */
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <xml platform file> <workflow file>" << std::endl;
        exit(1);
    }

    /* The first argument is the platform description file, written in XML following the SimGrid-defined DTD */
    char *platform_file = argv[1];
    /* The second argument is the workflow description file, written in XML using the DAX DTD */
    char *workflow_dir = argv[2];

    /* Reading and parsing the workflow description file to create a wrench::Workflow object */
    std::cerr << "Loading workflow..." << std::endl;

    /* Return a vector of workflow files from the workflow directory */
    std::vector<std::string> workflow_files = getAllFilesInDir(workflow_dir);

    /* Reading and parsing the platform description file to instantiate a simulated platform */
    std::cerr << "Instantiating SimGrid platform..." << std::endl;
    simulation.instantiatePlatform(platform_file);

    /* Get a vector of all the hosts in the simulated platform */
    std::vector<std::string> hostname_list = wrench::Simulation::getHostnameList();

    /* Create a list of storage services that will be used by the WMS */
    std::set<std::shared_ptr<wrench::StorageService>> storage_services;

    /* Instantiate a storage service, to be started on some host in the simulated platform,
     * and adding it to the simulation.  A wrench::StorageService is an abstraction of a service on
     * which files can be written and read.  This particular storage service, which is an instance
     * of wrench::SimpleStorageService, is started on Host3 in the
     * platform (platform/batch_platform.xml), which has an attached disk called large_disk. The SimpleStorageService
     * is a barebone storage service implementation provided by WRENCH.
     * Throughout the simulation execution, input/output files of workflow tasks will be located
     * in this storage service.
     */
    std::string storage_host = "WMSHost";
    std::cerr << "Instantiating a SimpleStorageService on " << storage_host << "..." << std::endl;
    auto storage_service = simulation.add(new wrench::SimpleStorageService(storage_host, {"/"}, {{wrench::SimpleStorageServiceProperty::BUFFER_SIZE, "50000000"}}, {}));
    storage_services.insert(storage_service);

    /* Construct a list of hosts (in the example only one host) on which the
     * cloud service will be able to run tasks
     */
    std::string executor_host1 = "VirtualizedClusterHost1";
    std::string executor_host2 = "VirtualizedClusterHost2";
    std::vector<std::string> execution_hosts = {executor_host1, executor_host2};

    /* Create a list of compute services that will be used by the WMS */
    std::set<std::shared_ptr<wrench::ComputeService>> compute_services;

    /* Instantiate a cloud service, to be started on some host in the simulation platform.
     * A cloud service is an abstraction of a compute service that corresponds to a
     * Cloud platform that provides access to virtualized compute resources.
     * In this example, this particular cloud service has no scratch storage space (mount point = "").
     * The last argument to the constructor
     * shows how to configure particular simulated behaviors of the compute service via a property
     * list. In this example, one specified that the message that will be send to the service to
     * terminate it will by 1024 bytes. See the documentation to find out all available
     * configurable properties for each kind of service.
     */
    std::string wms_host = "VirtualizedClusterProviderHost";

    /* Add the cloud service to the simulation, catching a possible exception */
    try {
        auto cluster_service = simulation.add(new wrench::VirtualizedClusterComputeService(
                wms_host, execution_hosts, "", {},
                {}));
        compute_services.insert(cluster_service);
    } catch (std::invalid_argument &e) {
        std::cerr << "Error: " << e.what() << std::endl;
        std::exit(1);
    }

    /* Add bandwidth meter service*/
    /* std::string bandwidth_meter_host = "VirtualizedClusterProviderHost";
    std::vector<std::string> links = simulation.getLinknameList();
    std::cerr << "Instantiating a BandwidthMeterService for all links..." << std::endl;
    wrench::BandwidthMeterService *bandwidth_meter_service =
	new wrench::BandwidthMeterService(bandwidth_meter_host, links, 0.01);
    simulation.add(bandwidth_meter_service); */
    // auto bandwidth_meter_service = simulation.add(new wrench::BandwidthMeterService(bandwidth_meter_host, links, 1));
    //  simulation.getOutput().enableBandwidthTimestamps(true);

    /* For each workflow file, create a workflow and instantiate a WMS */
    std::vector<std::shared_ptr<wrench::WMS>> wms_masters;
    std::vector<wrench::Workflow*> workflows;
    for (auto workflow_file : workflow_files) {
	if (ends_with(workflow_file, "dax")) {
	    wms_masters.push_back(simulation.add(
            	new wrench::SimpleWMS(std::unique_ptr<wrench::CloudStandardJobScheduler>(
                    	new wrench::CloudStandardJobScheduler(storage_service)),
                                      nullptr, compute_services, storage_services, wms_host)));
	    workflows.push_back(wrench::PegasusWorkflowParser::createWorkflowFromDAX(workflow_file, "100Gf"));
	    wms_masters.back()->addWorkflow(workflows.back());
	} else if (ends_with(workflow_file, "json")) {
	    wms_masters.push_back(simulation.add(
                new wrench::SimpleWMS(std::unique_ptr<wrench::CloudStandardJobScheduler>(
                        new wrench::CloudStandardJobScheduler(storage_service)),
                                      nullptr, compute_services, storage_services, wms_host)));
            workflows.push_back(wrench::PegasusWorkflowParser::createWorkflowFromJSON(workflow_file, "100Gf"));
            wms_masters.back()->addWorkflow(workflows.back(), workflows.back()->getSubmittedTime());
	    std::cerr << "WMS defer time is " << workflows.back()->getSubmittedTime() << 's' << std::endl;
	} else {
	    std::cerr << "Workflow file name must end with '.dax' or '.json'" << std::endl;
	}
    }
    if (wms_masters.empty() || workflows.empty()) {
    	exit(1);
    }
    std::cerr << "Successfully read " << workflows.size() << " workflows " << std::endl;
    std::cerr.flush();

    /* Instantiate a file registry service to be started on some host. This service is
     * essentially a replica catalog that stores <file , storage service> pairs so that
     * any service, in particular a WMS, can discover where workflow files are stored.
     */
    std::string file_registry_service_host = "WMSHost";
    std::cerr << "Instantiating a FileRegistryService on " << file_registry_service_host << "..." << std::endl;
    wrench::FileRegistryService *file_registry_service =
            new wrench::FileRegistryService(file_registry_service_host);
    simulation.add(file_registry_service);

    // TRYING NETWORK PROXIMITY SERVICE WITH VIVALDI....
    /* std::vector<std::string> hostname_list_copy(hostname_list);
    std::string hostname_copy(hostname_list[0]);
    wrench::NetworkProximityService *NPS = new wrench::NetworkProximityService(hostname_copy, hostname_list_copy, {
            {wrench::NetworkProximityServiceProperty::NETWORK_PROXIMITY_SERVICE_TYPE,                 "alltoall"},
            {wrench::NetworkProximityServiceProperty::NETWORK_DAEMON_COMMUNICATION_COVERAGE,          "1.0"},
            {wrench::NetworkProximityServiceProperty::NETWORK_PROXIMITY_MEASUREMENT_PERIOD,           "600"},
            {wrench::NetworkProximityServiceProperty::NETWORK_PROXIMITY_MEASUREMENT_PERIOD_MAX_NOISE, "10"}});

    simulation.add(NPS); */
    /* It is necessary to store, or "stage", input files for the first task(s) of the workflow on some storage
     * service, so that workflow execution can be initiated. The getInputFiles() method of the Workflow class
     * returns the set of all workflow files that are not generated by workflow tasks, and thus are only input files.
     * These files are then staged on the storage service.
     */
    std::cerr << "Staging input files..." << std::endl;
    for (auto workflow : workflows) {
    	for (auto const &f : workflow->getInputFiles()) {
            try {
            	simulation.stageFile(f, storage_service);
            } catch (std::runtime_error &e) {
            	std::cerr << "Exception: " << e.what() << std::endl;
            	return 0;
            }
    	}
    }


    /* Launch the simulation. This call only returns when the simulation is complete. */
    std::cerr << "Launching the Simulation..." << std::endl;
    try {
        simulation.launch();
    } catch (std::runtime_error &e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 0;
    }
    std::cerr << "Simulation done!" << std::endl;

    /* Simulation results can be examined via simulation.output, which provides access to traces
     * of events. In the code below, we retrieve the trace of all task completion events, print how
     * many such events there are, and print some information for the first such event.
     */
    
    /* Task completion trace*/
    std::vector<wrench::SimulationTimestamp<wrench::SimulationTimestampTaskCompletion> *> trace;
    trace = simulation.getOutput().getTrace<wrench::SimulationTimestampTaskCompletion>();
    
    for (auto const &item : trace) {
        std::cerr << "Job " << item->getContent()->getTask()->getWorkflow()->getName() << " - Task "  << item->getContent()->getTask()->getID() << " completed at time " << item->getDate()  << std::endl;
    }

    std::cerr << "Number of entries in TaskCompletion trace: " << trace.size() << std::endl;

    /* Link usage trace*/
    /* std::vector<wrench::SimulationTimestamp<wrench::SimulationTimestampLinkUsage> *> link_trace;
    link_trace = simulation.getOutput().getTrace<wrench::SimulationTimestampLinkUsage>();
    
    for (auto const &item : link_trace) {
        std::cerr << "Link " << item->getContent()->getLinkname() << " is utilized at "  << item->getContent()->getUsage() << " at time " << item->getDate()  << std::endl;
    } */

    for (int i = 0; i < workflows.size(); i++) {
	simulation.getOutput().dumpUnifiedJSON(workflows.at(i), "output/workflow_" + std::to_string(i) + "_exe.json", false, true, false, false, false, false, true);
	simulation.getOutput().dumpWorkflowGraphJSON(workflows.at(i), "output/workflow_" + std::to_string(i) + "_graph.json");
    }
    simulation.getOutput().dumpLinkUsageJSON("output/link_usage.json");
    simulation.getOutput().dumpPlatformGraphJSON("output/platform_graph.json");
    return 0;
}
