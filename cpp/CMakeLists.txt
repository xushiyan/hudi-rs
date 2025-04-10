cmake_minimum_required(VERSION 3.28)
project(hudi-cpp VERSION 0.4.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find the cargo metadata to locate the project paths
execute_process(
    COMMAND cargo metadata --format-version=1
    OUTPUT_VARIABLE CARGO_METADATA
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/..
)
string(JSON CARGO_WORKSPACE_ROOT GET ${CARGO_METADATA} "workspace_root")
string(JSON TARGET_DIRECTORY GET ${CARGO_METADATA} "target_directory")

# Set the default build type if not specified
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()

# Determine library path based on build type
if(CMAKE_BUILD_TYPE MATCHES "Debug")
    set(RUST_LIB_PATH "${TARGET_DIRECTORY}/debug/libhudi.dylib")
    set(CARGO_BUILD_COMMAND "cargo" "build")
else()
    set(RUST_LIB_PATH "${TARGET_DIRECTORY}/release/libhudi.dylib")
    set(CARGO_BUILD_COMMAND "cargo" "build" "--release")
endif()

# Include directories for the build
set(CXX_GENERATED_DIR "${TARGET_DIRECTORY}/cxxbridge")
set(INCLUDE_DIRS
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${CXX_GENERATED_DIR}"
)

# Custom command to build the rust library
add_custom_command(
    OUTPUT ${RUST_LIB_PATH}
    COMMAND ${CARGO_BUILD_COMMAND}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building Rust library with cargo"
    VERBATIM
)

# Create a custom target that depends on the Rust library
add_custom_target(rust_build ALL DEPENDS ${RUST_LIB_PATH})

# Add library target
add_library(hudi SHARED IMPORTED GLOBAL)
set_target_properties(hudi PROPERTIES
    IMPORTED_LOCATION ${RUST_LIB_PATH}
    INTERFACE_INCLUDE_DIRECTORIES "${INCLUDE_DIRS}"
)
add_dependencies(hudi rust_build)

# Install the library
install(FILES ${RUST_LIB_PATH}
    DESTINATION lib
)

# Install the header files
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/
    DESTINATION include/hudi
    FILES_MATCHING PATTERN "*.h"
)

# Install the generated headers (using custom command to handle symlinks)
add_custom_target(copy_generated_headers ALL
    DEPENDS rust_build
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/include_copy
    COMMAND ${CMAKE_COMMAND} -E copy_directory ${CXX_GENERATED_DIR} ${CMAKE_CURRENT_BINARY_DIR}/include_copy
    COMMENT "Copying generated headers to staging directory"
)

install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/include_copy/
    DESTINATION include
    FILES_MATCHING
        PATTERN "*.h"
        PATTERN "*.cc"
)

# Output configuration summary
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
message(STATUS "Rust library path: ${RUST_LIB_PATH}")
message(STATUS "Generated C++ path: ${CXX_GENERATED_DIR}")
message(STATUS "Install prefix: ${CMAKE_INSTALL_PREFIX}")