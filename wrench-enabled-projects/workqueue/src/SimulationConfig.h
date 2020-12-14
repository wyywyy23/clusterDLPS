/**
 * Copyright (c) 2019-2020. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef WORKQUEUE_SIMULATIONCONFIG_H
#define WORKQUEUE_SIMULATIONCONFIG_H

#include <nlohmann/json.hpp>
#include <wrench-dev.h>

namespace wrench {
    namespace workqueue {

        class SimulationConfig {
        public:
            void loadProperties(wrench::Simulation &simulation, const std::string &filename);

            std::string getSubmitHostname();

            std::string getFileRegistryHostname();

            std::shared_ptr<HTCondorComputeService> getHTCondorComputeService();

            std::set<std::shared_ptr<StorageService>> getStorageServices();

        private:
            void instantiateBatch(std::string service_host, std::vector<std::string> hosts);

            /**
             * @brief Get the value for a key from the JSON properties file
             *
             * @tparam T
             * @param keyName: property key that will be searched for
             * @param jsonData: JSON object to extract the value for the provided key
             * @param required: whether the property must exist
             *
             * @throw std::invalid_argument
             * @return The value for the provided key, if present
             */
            template<class T>
            T getPropertyValue(const std::string &keyName, const nlohmann::json &jsonData, const bool required = true) {
              if (jsonData.find(keyName) == jsonData.end()) {
                if (required) {
                  throw std::invalid_argument("SimulationConfig::loadProperties(): Unable to find " + keyName);
                } else {
                  return T();
                }
              }
              return jsonData.at(keyName);
            }

            std::string submit_hostname;
            std::string file_registry_hostname;
            std::set<ComputeService *> compute_services;
            std::set<std::shared_ptr<StorageService>> storage_services;
            std::shared_ptr<HTCondorComputeService> htcondor_service;
        };
    }
}

#endif //WORKQUEUE_SIMULATIONCONFIG_H
