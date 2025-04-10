cmake_minimum_required(VERSION 3.28)
project(hudi_cpp_examples)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find the installed hudi library
find_library(HUDI_LIBRARY hudi REQUIRED)

# Set the include directories directly to match the installation layout
set(HUDI_INCLUDE_DIR "/usr/local/include")

# Add the example executable
add_executable(hudi_cpp_example
  example.cpp
  ${HUDI_INCLUDE_DIR}/hudi/src/bridge.rs.cc
)

# The header paths need to be adjusted to match how they were installed
target_include_directories(hudi_cpp_example PRIVATE
  ${HUDI_INCLUDE_DIR}/hudi
  ${HUDI_INCLUDE_DIR}/rust
)

target_link_libraries(hudi_cpp_example ${HUDI_LIBRARY})

# Output configuration
message(STATUS "Using HUDI_LIBRARY: ${HUDI_LIBRARY}")
message(STATUS "Using HUDI_INCLUDE_DIR: ${HUDI_INCLUDE_DIR}")