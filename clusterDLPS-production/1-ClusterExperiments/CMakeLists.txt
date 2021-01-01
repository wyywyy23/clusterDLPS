cmake_minimum_required(VERSION 3.2)
message(STATUS "Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")

project(ToyProject)

# find SimGrid
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")
find_package(SimGrid 3.22 REQUIRED) # This template requires SimGrid v3.22

include_directories(src/ include/ /usr/local/include /usr/local/include/wrench /usr/local/include/wrench/tools/pegasus ${SimGrid_INCLUDE_DIR})

# wrench libraries
find_library(WRENCH_LIBRARY NAMES wrench)
find_library(WRENCH_PEGASUS_LIBRARY NAMES wrenchpegasusworkflowparser)
find_library(SimGrid_LIBRARY NAMES simgrid)
find_library(PUGIXML_LIBRARY NAMES pugixml)
find_library(GTEST_LIBRARY NAMES gtest)
find_library(ZMQ_LIBRARY NAMES zmq)

# experimental/filesystem linkers
set(FILESYSTEM_LIBRARY "stdc++fs")

# source files
set(SOURCE_FILES
	helper/getAllFilesInDir.h
        optimizations/static/SimplePipelineClustering.h
        optimizations/static/SimplePipelineClustering.cpp
        optimizations/dynamic/FailureDynamicClustering.h
        optimizations/dynamic/FailureDynamicClustering.cpp
        SimpleWMS.h
        SimpleWMS.cpp
        scheduler/CloudStandardJobScheduler.h
        scheduler/CloudStandardJobScheduler.cpp
        scheduler/BatchStandardJobScheduler.cpp
        scheduler/BatchStandardJobScheduler.h
        scheduler/pilot_job/CriticalPathPilotJobScheduler.h
        scheduler/pilot_job/CriticalPathPilotJobScheduler.cpp
        )

# multi-wms example
set(APP_MULTIWMS_FILES ClusterMultipleWMS.cpp)
add_executable(cluster-multiple-wms ${SOURCE_FILES} ${APP_MULTIWMS_FILES})
target_link_libraries(cluster-multiple-wms wrenchpegasusworkflowparser wrench ${SimGrid_LIBRARY} ${PUGIXML_LIBRARY} ${ZMQ_LIBRARY} ${FILESYSTEM_LIBRARY})

install(TARGETS cluster-multiple-wms DESTINATION bin)

