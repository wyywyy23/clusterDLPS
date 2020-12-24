#include <iostream>
#include <fstream>
#include <locale>
#include <math.h>
#include <pugixml.hpp>

int main(int argc, char **argv) {

    if (argc != 2) {
	std::cerr << "Usage: " << argv[0] << " <number of machines>" << std::endl;
        exit(1);
    }

    int num_machine = 4;
    try {
        num_machine = std::atoi(argv[1]);
    } catch (std::invalid_argument &e) {
        std::cerr << "Invalid number of machines. Must be 2^{2:12}." << std::endl;
        exit(1);
    }

    if (log2(num_machine) < 2 || log2(num_machine) > 12 || log2(num_machine) != (int) log2(num_machine)) {
        std::cerr << "Invalid number of machines. Must be 2^{2:12}." << std::endl;
        exit(1);
    }

    std::string topology = "fat_tree";
    std::string topo_name = "";
    std::locale loc;
    for (std::string::size_type i = 0; i < topology.length(); ++i) {
	topo_name.push_back(std::toupper(topology[i], loc));
    }

    std::string outfile_path = "platforms/cluster_" + std::to_string(num_machine) + "_machines_" + topo_name + ".xml";

    pugi::xml_document doc;

    auto declaration = doc.prepend_child(pugi::node_declaration);
    declaration.append_attribute("version") = "1.0";
    auto comment = doc.append_child(pugi::node_doctype).set_value("platform SYSTEM \"https://simgrid.org/simgrid.dtd\"");

    auto platform = doc.append_child("platform");
    platform.append_attribute("version") = "4.1";
    
    auto zone = platform.append_child("zone");
    zone.append_attribute("id")      = "Intra-cluster";
    zone.append_attribute("routing") = "Full";

    std::string radical = "0-" + std::to_string(num_machine - 1);
    auto cluster = zone.append_child("cluster");
    cluster.append_attribute("id")              = "Alibaba";
    cluster.append_attribute("prefix")          = "node-";
    cluster.append_attribute("radical")         = radical.c_str();
    cluster.append_attribute("suffix")          = ".example.org";
    cluster.append_attribute("speed")           = "1Gf";
    cluster.append_attribute("core")            = "96";
    cluster.append_attribute("bw")              = "100Gbps";
    cluster.append_attribute("lat")             = "20us";
    cluster.append_attribute("topology")        = topo_name.c_str();
    cluster.append_attribute("topo_parameters") = "2;4,4;1,2;1,2";
    cluster.append_attribute("loopback_bw")     = "1000EBps";
    cluster.append_attribute("loopback_lat")    = "0";

    // auto host = cluster.append_child("host");


    std::ofstream file;
    file.exceptions(std::ofstream::failbit | std::ofstream::badbit);
    try {
        file.open(outfile_path);
        doc.save(file, "    ");
        file.close();
    } catch (const std::ofstream::failure &e) {
        throw std::invalid_argument("Cannot open file for output!");
    }

    return 0;
}
