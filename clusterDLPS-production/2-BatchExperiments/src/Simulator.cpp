#include <iostream>
#include <wrench-dev.h>
#include "Simulator.h"
#include "helper/getAllFilesInDir.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(wyy_simulator, "Log category for the main simulator");

using namespace wyy;

int Simulator::run(int argc, char** argv) {

    wrench::Simulation simulation;
    simulation.init(&argc, argv);

    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <platform file> <background trace file> <workflow directory>" << std::endl;
        exit(1);
    }

    char* platform_file = argv[1];
    char* background_trace_file = argv[2];
    char* workflow_dir = argv[3];

    std::cerr << "Instantiating a platform..." << std::endl;
    simulation.instantiatePlatform(platform_file);

    /* Instantiate one compute service on the WMS host for all nodes */
    std::vector<std::string> hostname_list = wrench::Simulation::getHostnameList();

    /* Debug */
    for (auto hostname: hostname_list) {
	WRENCH_DEBUG("Host: %s\tCores: %ld", hostname.c_str(), wrench::Simulation::getHostNumCores(hostname));
    }
    /* std::vector<std::string> linkname_list = wrench::Simulation::getLinknameList();
    for (auto linkname: linkname_list) {
	WRENCH_DEBUG("Link: %s\tBW: %f", linkname.c_str(), wrench::Simulation::getLinkBandwidth(linkname));
    } */

    /* Instantiate one file registry service on the WMS host */

    /* Instantiate a simple storage service for each node */

    /* Load all workflows from folder  *
     * Each workflow instantiate a WMS */
    std::cerr << "Loading workflows..." << std::endl;
    std::vector<std::string> workflow_files = getAllFilesInDir(workflow_dir);


    std::vector<shared_ptr<wrench::WMS>> wms_masters;

    return 0;
}

