cmake_minimum_required(VERSION 3.15)
project(ws_distro LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Prefer Release by default
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# Boost.Beast is header-only; Boost.System is required for linking
find_package(Boost 1.75 REQUIRED COMPONENTS system)

add_executable(ws_distro
  src/main.cpp
)

target_include_directories(ws_distro PRIVATE ${Boost_INCLUDE_DIRS})

target_link_libraries(ws_distro PRIVATE ${Boost_LIBRARIES})

# macOS: ensure rpaths are set sensibly
if(APPLE)
  set_target_properties(ws_distro PROPERTIES
    MACOSX_RPATH ON
  )
endif()
