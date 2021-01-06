#include <iostream>
#include <wrench-dev.h>
#include <set>
#include <map>
#include "Simulator.h"
#include "wyyWMS.h"
#include "BatchStandardJobScheduler.h"
#include "helper/getAllFilesInDir.h"
#include "helper/endWith.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(wyy_simulator, "Log category for the main simulator");

using namespace wyy;

int Simulator::run(int argc, char** argv) {

    auto simulation = new wrench::Simulation();
    simulation->init(&argc, argv);

    if (argc != 5 and argc != 6) {
        std::cerr << "Usage: " << argv[0] << " <platform file> <background trace file> <workflow directory> <scheduling algorithm> [host selection algorithm]" << std::endl;
        exit(1);
    }

    char* platform_file = argv[1];

    try {
	simulation->instantiatePlatform(platform_file);
    } catch (std::invalid_argument &e) {
	std::cerr << "Cannot instantiate the platform: " << e.what() << std::endl;
	exit(1);
    }
    std::cerr << "Instantiated a platform." << std::endl;

    /* Instantiate one compute service on the WMS host for all nodes */
    std::vector<std::string> hostname_list = simulation->getHostnameList();
    std::vector<std::string> compute_nodes = hostname_list;
    std::string master_node = "master";
    for (auto it = compute_nodes.begin(); it != compute_nodes.end(); ++it) {
	if (*it == master_node) {
	    compute_nodes.erase(it);
	}
    }

    /* Debug */
    WRENCH_DEBUG("Current computation nodes in simulation:");
    for (auto hostname: compute_nodes) {
	WRENCH_DEBUG("Host: %s\tCores: %ld", hostname.c_str(), simulation->getHostNumCores(hostname));
    }
    /* std::vector<std::string> linkname_list = wrench::Simulation::getLinknameList();
    for (auto linkname: linkname_list) {
	WRENCH_DEBUG("Link: %s\tBW: %f", linkname.c_str(), wrench::Simulation::getLinkBandwidth(linkname));
    } */

    std::set<std::shared_ptr<wrench::ComputeService>> compute_services;
    wrench::BatchComputeService* temp_batch_service = nullptr;
    try {
	temp_batch_service = new wrench::BatchComputeService(
		master_node, compute_nodes, "", {
		{wrench::BatchComputeServiceProperty::BATCH_SCHEDULING_ALGORITHM, std::string(argv[4])},
		{wrench::BatchComputeServiceProperty::BATSCHED_CONTIGUOUS_ALLOCATION, "true"},
		{wrench::BatchComputeServiceProperty::BATSCHED_LOGGING_MUTED, "true"},
		{wrench::BatchComputeServiceProperty::HOST_SELECTION_ALGORITHM, argc == 5 ? "FIRSTFIT" : std::string(argv[5])},
		{wrench::BatchComputeServiceProperty::IGNORE_INVALID_JOBS_IN_WORKLOAD_TRACE_FILE, "true"},
		{wrench::BatchComputeServiceProperty::OUTPUT_CSV_JOB_LOG, "/tmp/batch_log.csv"},
		{wrench::BatchComputeServiceProperty::SIMULATE_COMPUTATION_AS_SLEEP, "true"},
		{wrench::BatchComputeServiceProperty::SIMULATED_WORKLOAD_TRACE_FILE, std::string(argv[2])},
		{wrench::BatchComputeServiceProperty::SUBMIT_TIME_OF_FIRST_JOB_IN_WORKLOAD_TRACE_FILE, "-1"},
		{wrench::BatchComputeServiceProperty::TASK_SELECTION_ALGORITHM, "maximum_flops"},
		{wrench::BatchComputeServiceProperty::TASK_STARTUP_OVERHEAD, "0"},
		{wrench::BatchComputeServiceProperty::USE_REAL_RUNTIMES_AS_REQUESTED_RUNTIMES_IN_WORKLOAD_TRACE_FILE, "true"}
		}, {

		}
	);
    } catch (std::invalid_argument &e) {
	std::cerr << "Cannot instantiate a batch service: " << e.what() << std::endl;
	exit(1);
    }
    compute_services.insert(simulation->add(temp_batch_service));
    std::cerr << "Instantiated a batch compute service on " << temp_batch_service->getHostname() <<" with scheduling algorithm: " << temp_batch_service->getPropertyValueAsString(wrench::BatchComputeServiceProperty::BATCH_SCHEDULING_ALGORITHM) << "." << std::endl;

    /* Debug */
    WRENCH_DEBUG("Current compute services in simulation:");
    for (auto com_serv : compute_services) {
	WRENCH_DEBUG("On node: %s\tIs up: %s", com_serv->getHostname().c_str(), com_serv->isUp() ? "true" : "false");
    }

    /* Instantiate one file registry service on the WMS host */
    std::set<std::shared_ptr<wrench::FileRegistryService>> file_registry_services;
    wrench::FileRegistryService* temp_file_registry_service = nullptr;
    try {	
	temp_file_registry_service = new wrench::FileRegistryService(master_node);
    } catch (std::invalid_argument &e) {
	std::cerr << "Cannot instantiate a file registry service: " << e.what() << std::endl;
        exit(1);
    }
    file_registry_services.insert(simulation->add(temp_file_registry_service));
    std::cerr << "Instantiated a file registry service on " << temp_file_registry_service->getHostname() << "." << std::endl;

    /* Debug */
    WRENCH_DEBUG("Current file registry services in simulation:");
    for (auto fr_serv : file_registry_services) {
	WRENCH_DEBUG("On node: %s\tIs up: %s", fr_serv->getHostname().c_str(), fr_serv->isUp() ? "true" : "false");
    }

    /* Instantiate a simple storage service for each node */
    std::set<std::shared_ptr<wrench::StorageService>> storage_services;
    wrench::SimpleStorageService* temp_storage_service = nullptr;
    try {
	temp_storage_service = new wrench::SimpleStorageService(master_node, {"/"}, {
		{wrench::SimpleStorageServiceProperty::BUFFER_SIZE, "infinity"}
		}, {

		}
	);
    } catch (std::invalid_argument &e) {
	std::cerr << "Cannot instantiate a storage service: " << e.what() << std::endl;
	exit(1);
    }
    storage_services.insert(simulation->add(temp_storage_service));
    std::cerr << "Instantiated a storage service on " << temp_storage_service->getHostname() << "." << std::endl;

    /* Debug */
    WRENCH_DEBUG("Current storage services in simulation:");
    for (auto st_serv : storage_services) {
	WRENCH_DEBUG("On node: %s\tIs up: %s", st_serv->getHostname().c_str(), st_serv->isUp() ? "true" : "false");
    }

    /* Load all workflows from folder  *
     * Each workflow instantiate a WMS */
    std::cerr << "Loading workflows..." << std::endl;
    std::vector<std::string> workflow_files = getAllFilesInDir(std::string(argv[3]));

    std::set<shared_ptr<wrench::WMS>> wms_services;
    // std::map<std::shared_ptr<wrench::WMS>, std::shared_ptr<wrench::Workflow>> wms_workflow_map;

    for (auto workflow_file: workflow_files) {
	wrench::Workflow* temp_workflow = createWorkflowFromFile(workflow_file);
	if (!temp_workflow) {
	    std::cerr << "Workflow creation failed." << std::endl;
	    exit(1);
	}
	WRENCH_DEBUG("Created a workflow from %s, submit time: %d.", workflow_file.c_str(), temp_workflow->getSubmittedTime());

	wrench::WMS* temp_wms = nullptr;
	auto default_storage = *storage_services.begin();
	try {
	    temp_wms = new wrench::wyyWMS(
		    std::unique_ptr<wrench::BatchStandardJobScheduler> (new wrench::BatchStandardJobScheduler(default_storage)),
		    nullptr, compute_services, storage_services, master_node
	    );
	} catch (std::invalid_argument &e) {
	    std::cerr << "Cannot instantiate a WMS: " << e.what() << std::endl;
	    exit(1);
	}
	WRENCH_DEBUG("Instantiated a WMS for %s, default storage: %s", workflow_file.c_str(), default_storage->getHostname().c_str());
	temp_wms->addWorkflow(temp_workflow, temp_workflow->getSubmittedTime());
	wms_services.insert(simulation->add(temp_wms));
    }
    std::cerr << "Instantiated a WMS for each workflow generated." << std::endl;

    /* Debug */
    WRENCH_DEBUG("Current WMS services in simulation:");
    for (auto wms_serv : wms_services) {
	WRENCH_DEBUG("On node: %s\tIs up: %s", wms_serv->getHostname().c_str(), wms_serv->isUp() ? "true" : "false");
    }

    /* Stage input files for all workflows */
    for (auto wms_serv : wms_services) {
	for (auto const &f : wms_serv->getWorkflow()->getInputFiles()) {
	    try {
		simulation->stageFile(f, *storage_services.begin());
		WRENCH_DEBUG("Staged input file %s", f->getID().c_str());
	    } catch (std::runtime_error &e) {
		std::cerr << "Cannot stage input file " << f->getID() << ": " << e.what() << std::endl;
		exit(1);
	    }
	}
    }
    std::cerr << "Staged input files to the master storage." << std::endl;

    /* Launch the simulation */
    std::cerr << "Launching the simulation..." << std::endl;
    try {
	simulation->launch();
    } catch (std::runtime_error &e) {
	std::cerr << "Simulation failed: " << e.what() << std::endl;
	exit(1);
    }

    std::cerr << "Simulation done!" << std::endl;

    /* Workflow completion info */ 
    for (auto wms_serv : wms_services) {
	WRENCH_DEBUG("Job %s\tmakespan %f", wms_serv->getWorkflow()->getName().c_str(), wms_serv->getWorkflow()->getCompletionDate() - wms_serv->getWorkflow()->getSubmittedTime());
	simulation->getOutput().dumpUnifiedJSON(wms_serv->getWorkflow(), "output/workflow_" + wms_serv->getWorkflow()->getName() + "_unified.json", false, true, false, false, false, false, false);
    }

    /* Task completion trace */
    std::vector<wrench::SimulationTimestamp<wrench::SimulationTimestampTaskCompletion> *> trace;
    trace = simulation->getOutput().getTrace<wrench::SimulationTimestampTaskCompletion>();
    WRENCH_INFO("Number of entries in TaskCompletion trace: %ld", trace.size());

    return 0;
}

wrench::Workflow* Simulator::createWorkflowFromFile(std::string& workflow_file) {

    wrench::Workflow* workflow = nullptr;

    if (endWith(workflow_file, "json")) {
	try {
	    workflow = wrench::PegasusWorkflowParser::createWorkflowFromJSON(workflow_file, "1f");
	} catch (std::invalid_argument &e) {
	    std::cerr << "Cannot create a workflow from " << workflow_file << ": " << e.what() << std::endl;
	}
    } else if (endWith(workflow_file, "dax")) {
	try {
	    workflow = wrench::PegasusWorkflowParser::createWorkflowFromDAX(workflow_file, "1f");
	} catch (std::invalid_argument &e) {
	    std::cerr << "Cannot create a workflow from " << workflow_file << ": " << e.what() << std::endl;
	}

    } else {
	std::cerr << "Cannot create a workflow from " << workflow_file << ": not supporting formats other than .json and .dax" << std::endl;
    }

    return workflow;
}

