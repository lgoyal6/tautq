cmake_minimum_required(VERSION 3.24)
project(tautq LANGUAGES CXX VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Full-path includes, NOT CMAKE_MODULE_PATH: the vendored taut also include()s files named
# Warnings/Sanitizers, and an inherited module path would make it load ours instead.
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/Warnings.cmake")
include("${CMAKE_CURRENT_SOURCE_DIR}/cmake/Sanitizers.cmake") # before taut is added, so the
                                                              # library is instrumented too

option(TAUTQ_BUILD_TESTS "Build tautq tests" ON)

# Vendored taut (D8). Local dev points at the sibling checkout; CI overrides
# TAUTQ_TAUT_DIR or switches to FetchContent once taut is fetchable (open item in D8).
set(TAUTQ_TAUT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../taut" CACHE PATH "Path to a taut checkout")
set(TAUT_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(TAUT_BUILD_FUZZERS OFF CACHE BOOL "" FORCE)
add_subdirectory("${TAUTQ_TAUT_DIR}" taut EXCLUDE_FROM_ALL)

# Core library: everything the binaries share. No third-party deps (test deps via
# FetchContent only), same bar as taut.
add_library(tautq_core STATIC
    src/wire.cc
    src/demux.cc
    src/rpc.cc
    src/records.cc
    src/wal.cc
)
target_include_directories(tautq_core PUBLIC src)
target_compile_features(tautq_core PUBLIC cxx_std_20)
target_link_libraries(tautq_core PUBLIC taut PRIVATE tautq_warnings)

if(TAUTQ_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
