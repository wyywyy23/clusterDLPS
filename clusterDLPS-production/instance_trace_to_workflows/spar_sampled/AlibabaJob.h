#include <wrench-dev.h>


class AlibabaJob : public wrench::Workflow {

    public:
        AlibabaJob* updateJob(std::string task_name, std::string instance_name, double start_time, double duration, double avg_cpu, double avg_mem);
        std::set<std::pair<std::string, std::string>> getDependencyMap() {
                return this->dependencies; }
        std::multimap<std::string, std::string> getTaskInstanceMap() {
                return this->task_instances; }
	void addControlDependency(wrench::WorkflowTask* src, wrench::WorkflowTask* dest, bool redundant_dependencies = false);
        void printPairs(); // only for debugging
	double generateFileSize(double exe_time);

    private:
        std::set<std::pair<std::string, std::string>> dependencies;
        std::multimap<std::string, std::string> task_instances;

};
