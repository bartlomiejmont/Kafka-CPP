cmake_minimum_required(VERSION 3.14)
project(kf)

set(CMAKE_CXX_STANDARD 14)

add_executable(kf main.cpp)

include_directories("/usr/local/include/librdkafka")
include_directories("/usr/local/include/cppkafka")
FIND_LIBRARY(RDKAFKA rdkafka /usr/local/lib NO_DEFAULT_PATH)
FIND_LIBRARY(CPPKAFKA cppkafka /usr/local/lib NO_DEFAULT_PATH)
link_libraries(${RDKAFKA} ${CPPKAFKA})
target_link_libraries(kf cppkafka)
target_link_libraries(kf rdkafka)