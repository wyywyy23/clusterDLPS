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

    /* Load all workflows from folder  *
     * Each workflow instantiate a WMS */
    std::cerr << "Loading workflows..." << std::endl;
    std::vector<std::string> workflow_files = getAllFilesInDir(workflow_dir);


    std::vector<shared_ptr<wrench::WMS>> wms_masters;

    return 0;
}

