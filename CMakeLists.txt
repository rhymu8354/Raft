# CMakeLists.txt for Raft
#
# © 2018 by Richard Walters

cmake_minimum_required(VERSION 3.8)
set(This Raft)

set(Headers
    include/Raft/IServer.hpp
    include/Raft/LogEntry.hpp
    include/Raft/Message.hpp
    include/Raft/Server.hpp
    include/Raft/TimeKeeper.hpp
)

set(Sources
    src/Message.cpp
    src/Server.cpp
)

add_library(${This} STATIC ${Sources} ${Headers})
set_target_properties(${This} PROPERTIES
    FOLDER Libraries
)

target_include_directories(${This} PUBLIC include)

target_link_libraries(${This} PUBLIC
    SystemAbstractions
)

add_subdirectory(test)
