cmake_minimum_required(VERSION 3.2)
message(STATUS "Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")

project(wyy_simulator)
add_definitions("-Wall -Wno-unused-variable -Wno-unused-private-field")

# find SimGrid
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")
find_package(SimGrid 3.26 REQUIRED) # This template requires SimGrid v3.26

include_directories(src/ /usr/local/include /usr/local/include/wrench ${SimGrid_INCLUDE_DIR})

# wrench libraries
find_library(WRENCH_LIBRARY NAMES wrench)
find_library(WRENCH_PEGASUS_LIBRARY NAMES wrenchpegasusworkflowparser)
find_library(SimGrid_LIBRARY NAMES simgrid)
find_library(PUGIXML_LIBRARY NAMES pugixml)
find_library(ZMQ_LIBRARY NAMES zmq)

# experimental/filesystem linkers
set(FILESYSTEM_LIBRARY "stdc++fs")

# source files
set(SOURCE_FILES
	src/main.cpp
	src/Simulator.h
	src/Simulator.cpp
	src/helper/endWith.h
	src/helper/getAllFilesInDir.h
	)

# executable
add_executable(wyy_simulator ${SOURCE_FILES})

target_link_libraries(wyy_simulator
	${WRENCH_LIBRARY}
	${WRENCH_PEGASUS_LIBRARY}
	${SimGrid_LIBRARY}
	${PUGIXML_LIBRARY}
	${ZMQ_LIBRARY}
	${FILESYSTEM_LIBRARY}
	)
