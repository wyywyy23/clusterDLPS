cmake_minimum_required(VERSION 3.2)
message(STATUS "Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")

project(workqueue CXX)

add_definitions("-Wall -Wno-unused-variable -Wno-unused-private-field")

set(CMAKE_CXX_STANDARD 11)

# build the version number
set(WRENCH_WORKQUEUE_VERSION_MAJOR "1")
set(WRENCH_WORKQUEUE_VERSION_MINOR "6")
set(WRENCH_WORKQUEUE_VERSION_PATCH "0")
set(WRENCH_WORKQUEUE_VERSION_EXTRA "")

if (${WRENCH_WORKQUEUE_VERSION_PATCH} EQUAL "0")
    set(WRENCH_WORKQUEUE_RELEASE_VERSION "${WRENCH_WORKQUEUE_VERSION_MAJOR}.${WRENCH_WORKQUEUE_VERSION_MINOR}")
else ()
    set(WRENCH_WORKQUEUE_RELEASE_VERSION "$(WRENCH_WORKQUEUE_VERSION_MAJOR}.${WRENCH_WORKQUEUE_VERSION_MINOR}.${WRENCH_WORKQUEUE_VERSION_PATCH}")
endif ()

if (NOT ${WRENCH_WORKQUEUE_VERSION_EXTRA} EQUAL "")
    set(WRENCH_WORKQUEUE_RELEASE_VERSION "${WRENCH_WORKQUEUE_RELEASE_VERSION}-${WRENCH_WORKQUEUE_VERSION_EXTRA}")
endif ()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")
find_package(SimGrid 3.22 REQUIRED) # This template requires SimGrid v3.22

include_directories(src/ include/ /usr/local/include /usr/local/include/wrench ${SimGrid_INCLUDE_DIR})

# source files
set(SOURCE_FILES
        src/WorkQueue.cpp
        src/SimulationConfig.h
        src/SimulationConfig.cpp
        src/WorkQueueWMS.h
        src/WorkQueueWMS.cpp
        src/WorkQueueScheduler.h
        src/WorkQueueScheduler.cpp
        src/WorkQueueSimulationTimestampTypes.h
        src/WorkQueueSimulationTimestampTypes.cpp
        )

set(TEST_FILES
        )

# wrench library
find_library(WRENCH_LIBRARY NAMES wrench)
find_library(WRENCH_PEGASUS_LIBRARY NAMES wrenchpegasusworkflowparser)
find_library(SimGrid_LIBRARY NAMES simgrid)
find_library(PUGIXML_LIBRARY NAMES pugixml)
find_library(GTEST_LIBRARY NAMES gtest)

add_executable(wrench-workqueue ${SOURCE_FILES})
target_link_libraries(wrench-workqueue ${WRENCH_LIBRARY} ${WRENCH_PEGASUS_LIBRARY} ${SimGrid_LIBRARY} ${PUGIXML_LIBRARY})
install(TARGETS wrench-workqueue DESTINATION bin)
