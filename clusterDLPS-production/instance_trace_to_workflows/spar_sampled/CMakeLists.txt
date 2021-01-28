cmake_minimum_required(VERSION 3.2)
message(STATUS "Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")

project(TraceToWorkflows) # TODO: give a real name to your project here

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -lpthread")

# find SimGrid
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")
find_package(SimGrid 3.22 REQUIRED) # This template requires SimGrid v3.22

include_directories(src/ include/ /usr/local/include /usr/local/include/wrench ${SimGrid_INCLUDE_DIR})

# wrench libraries
find_library(WRENCH_LIBRARY NAMES wrench)
find_library(SimGrid_LIBRARY NAMES simgrid)

set(SOURCE_FILES
	AlibabaJob.h
	AlibabaJob.cpp
        helper/helper.h
        helper/splitString.cpp
	fast-cpp-csv-parser/csv.h
   )

set(MAIN_FILE trace_to_workflows.cpp)

add_executable(trace2workflows ${SOURCE_FILES} ${MAIN_FILE})
target_link_libraries(trace2workflows ${WRENCH_LIBRARY} ${SimGrid_LIBRARY})
