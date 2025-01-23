cmake_minimum_required(VERSION 3.14)
project(run_base)

include(FetchContent)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

SET(CMAKE_CXX_FLAGS "-O3")
# -lboost_log
set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
set(THREADS_PREFER_PTHREAD_FLAG ON)

# -DBOOST_LOG_DYN_LINK
# definitions
add_definitions(-DBOOST_ALL_NO_LIB)
add_definitions(-DBOOST_LOG_DYN_LINK=1)

# -lboost_log -lboost_thread -lboost_log_setup
find_package(Boost 1.74.0 COMPONENTS thread log log_setup REQUIRED)
find_package(PkgConfig)

pkg_check_modules(PROTOBUF REQUIRED IMPORTED_TARGET protobuf)
pkg_check_modules(SNAPPY REQUIRED IMPORTED_TARGET snappy)
pkg_check_modules(CURLPP REQUIRED IMPORTED_TARGET curlpp)
pkg_check_modules(LIBCURL REQUIRED IMPORTED_TARGET libcurl)

set(Parquet_DIR /usr/lib/x86_64-linux-gnu/cmake/arrow)

find_package(Threads REQUIRED)
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)
find_package(crossguid CONFIG REQUIRED)
find_package(aws-cpp-sdk-core CONFIG REQUIRED)
find_package(aws-cpp-sdk-s3 CONFIG REQUIRED)
find_package(Protobuf MODULE REQUIRED)



add_executable(run_base_simple
    src/run_base_simple.cpp 
)

target_link_libraries(run_base_simple PUBLIC hiredis redis++ fmt arrow_shared parquet_shared crossguid aws-cpp-sdk-core aws-cpp-sdk-s3 SimpleAmqpClient Threads::Threads PkgConfig::LIBCURL)

install(TARGETS run_base_simple RUNTIME DESTINATION /usr/local/bin)

