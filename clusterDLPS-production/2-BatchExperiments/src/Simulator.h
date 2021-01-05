#ifndef WYY_SIMULATOR_SIMULATOR_H
#define WYY_SIMULATOR_SIMULATOR_H

#include "wrench-dev.h"

namespace wyy {

    class Simulator {

    public:

        int run(int argc, char** argv);
	wrench::Workflow* createWorkflowFromFile(std::string &);
    };

}

#endif // WYY_SIMULATOR_SIMULATOR_H

