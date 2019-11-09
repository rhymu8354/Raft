# CMakeLists.txt for RaftTests
#
# © 2018 by Richard Walters

cmake_minimum_required(VERSION 3.8)
set(This RaftTests)

set(Sources
    src/ServerTests/Common.cpp
    src/ServerTests/Common.hpp
    src/ServerTests/Elections.cpp
    src/ServerTests/Reconfiguration.cpp
    src/ServerTests/Replication.cpp
    src/LogEntryTests.cpp
    src/MessageTests.cpp
)

add_executable(${This} ${Sources})
set_target_properties(${This} PROPERTIES
    FOLDER Tests
)

target_link_libraries(${This} PUBLIC
    gtest_main
    Json
    Raft
    StringExtensions
    SystemAbstractions
)

add_test(
    NAME ${This}
    COMMAND ${This}
)
