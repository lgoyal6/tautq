include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(googletest)

add_executable(tautq_unit
    unit/wire_test.cc
    unit/demux_test.cc
    unit/rpc_test.cc
    unit/records_test.cc
    unit/wal_test.cc
    unit/submit_test.cc
)
target_link_libraries(tautq_unit PRIVATE tautq_core tautq_test_warnings GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(tautq_unit)
