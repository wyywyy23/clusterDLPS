#include <iostream>
#include <fstream>
#include <locale>
#include <set>
#include <math.h>
#include <pugixml.hpp>

int main(int argc, char **argv) {

    if (argc != 3) {
	std::cerr << "Usage: " << argv[0] << " <topology> <number of machines>" << std::endl;
        exit(1);
    }

    int num_machine = 4;
    try {
        num_machine = std::atoi(argv[2]);
    } catch (std::invalid_argument &e) {
        std::cerr << "Invalid number of machines. Must be 2^{2:12}." << std::endl;
        exit(1);
    }

    if (log2(num_machine) < 2 || log2(num_machine) > 12 || log2(num_machine) != (int) log2(num_machine)) {
        std::cerr << "Invalid number of machines. Must be 2^{2:12}." << std::endl;
        exit(1);
    }

    std::string topology = argv[1];
    std::string topo_name = "";
    std::locale loc;
    for (std::string::size_type i = 0; i < topology.length(); ++i) {
	topo_name.push_back(std::toupper(topology[i], loc));
    }

    std::set<std::string> supported_topology = {"FAT_TREE"};
    if (supported_topology.find(topo_name) == supported_topology.end()) {
	std::cerr << "Invalid topology name. Currently support ";
        for (auto it = supported_topology.begin(); it != supported_topology.end(); ++it) {
	    std::cerr << "[" << *it << "] ";
	}
	std::cerr << std::endl;
	exit(1);
    }

    /* Prepare some parameters */
    std::string radical = "0-" + std::to_string(num_machine - 1);
    std::string topo_parameter;
    if (topo_name == "FAT_TREE") {
	double sr = sqrt(num_machine);
	int p, m;
	if (sr - floor(sr) == 0) {
	    p = 2 * (int) sr;
	    m = (int) sr; 
	} else {
	    p = 4 * (int) sqrt(num_machine / 2);
	    m = (int) sqrt(num_machine / 2);
	}
	topo_parameter = "2;" + std::to_string(p / 2) + "," + std::to_string(m) + ";1," + std::to_string(m / 2) + ";1," + std::to_string(p / m);
    }

    std::string outfile_path = "platforms/cluster_" + std::to_string(num_machine) + "_machines_" + topo_name + ".xml";
    std::cerr << "Generating: " << outfile_path << std::endl;

    pugi::xml_document doc;

    auto declaration = doc.prepend_child(pugi::node_declaration);
    declaration.append_attribute("version") = "1.0";
    auto comment = doc.append_child(pugi::node_doctype).set_value("platform SYSTEM \"https://simgrid.org/simgrid.dtd\"");

    auto platform = doc.append_child("platform");
    platform.append_attribute("version") = "4.1";
    
    auto big_zone = platform.append_child("zone");
    big_zone.append_attribute("id")      = "Cluster_and_a_host";
    big_zone.append_attribute("routing") = "Full";

    auto cluster_zone = big_zone.append_child("cluster");
    cluster_zone.append_attribute("id")              = "Computation";
    cluster_zone.append_attribute("prefix")          = "node-";
    cluster_zone.append_attribute("radical")         = radical.c_str();
    cluster_zone.append_attribute("suffix")          = "";
    cluster_zone.append_attribute("speed")           = "1f";
    cluster_zone.append_attribute("core")            = "96";
    cluster_zone.append_attribute("bw")              = "100Gbps";
    cluster_zone.append_attribute("lat")             = "20us";
    cluster_zone.append_attribute("topology")        = topo_name.c_str();
    cluster_zone.append_attribute("topo_parameters") = topo_parameter.c_str();
    cluster_zone.append_attribute("loopback_bw")     = "1000EBps";
    cluster_zone.append_attribute("loopback_lat")    = "0";
    cluster_zone.append_attribute("router_id")       = "cluster_router";

    // auto host = cluster.append_child("host");

    auto host_zone = big_zone.append_child("zone");
    host_zone.append_attribute("id")      = "Master";
    host_zone.append_attribute("routing") = "Full";

    auto host = host_zone.append_child("host");
    host.append_attribute("id")    = "master";
    host.append_attribute("speed") = "1f";

    auto disk = host.append_child("disk");
    disk.append_attribute("id")       = "cloud_disk";
    disk.append_attribute("read_bw")  = "4000MBps";
    disk.append_attribute("write_bw") = "4000MBps";

    auto disk_size = disk.append_child("prop");
    disk_size.append_attribute("id")    = "size";
    disk_size.append_attribute("value") = "32768GiB";

    auto disk_mount = disk.append_child("prop");
    disk_mount.append_attribute("id")    = "mount";
    disk_mount.append_attribute("value") = "/";

    auto host_loopback = host_zone.append_child("link");
    host_loopback.append_attribute("id")             = "loopback";
    host_loopback.append_attribute("bandwidth")      = "1000EBps";
    host_loopback.append_attribute("latency")        = "0us";
    host_loopback.append_attribute("sharing_policy") = "FATPIPE";

    auto host_route = host_zone.append_child("route");
    host_route.append_attribute("src") = "master";
    host_route.append_attribute("dst") = "master";
    host_route.append_child("link_ctn").append_attribute("id").set_value("loopback");

    auto fast_link = big_zone.append_child("link");
    fast_link.append_attribute("id")             = "fast_link";
    fast_link.append_attribute("bandwidth")      = "1000EBps";
    fast_link.append_attribute("latency")        = "0us";
    fast_link.append_attribute("sharing_policy") = "FATPIPE";

    auto zone_route = big_zone.append_child("zoneRoute");
    zone_route.append_attribute("src")    = "Computation";
    zone_route.append_attribute("dst")    = "Master";
    zone_route.append_attribute("gw_src") = "cluster_router";
    zone_route.append_attribute("gw_dst") = "master";
    zone_route.append_child("link_ctn").append_attribute("id").set_value("fast_link");

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
