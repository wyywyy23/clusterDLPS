#include "Simulator.h"

int main(int argc, char **argv) {

  wyy::Simulator *simulator = new wyy::Simulator();
  return simulator->run(argc, argv);
}

