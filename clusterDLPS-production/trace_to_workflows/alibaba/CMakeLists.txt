cmake_minimum_required(VERSION 2.8.8)

project(TraceToWorkflows) # TODO: give a real name to your project here

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -lpthread")

set(SOURCE_FILES
	fast-cpp-csv-parser/csv.h
   )

set(MAIN_FILE trace_to_workflows.cpp)

add_executable(trace2workflows ${SOURCE_FILES} ${MAIN_FILE})
