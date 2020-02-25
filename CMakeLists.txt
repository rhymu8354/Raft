# CMakeLists.txt for Raft
#
# © 2018 by Richard Walters

cmake_minimum_required(VERSION 3.8)
set(This Raft)

option(RAFT_EXTRA_DIAGNOSTICS "Report extra diagnostics (significant performance hit)" OFF)

set(Headers
    include/Raft/ClusterConfiguration.hpp
    include/Raft/Command.hpp
    include/Raft/ILog.hpp
    include/Raft/IPersistentState.hpp
    include/Raft/IServer.hpp
    include/Raft/LogEntry.hpp
    include/Raft/Server.hpp
    src/InstanceInfo.hpp
    src/Message.hpp
    src/ServerImpl.hpp
    src/Utilities.hpp
)

set(Sources
    src/LogEntry.cpp
    src/Message.cpp
    src/Server.cpp
    src/ServerImpl.cpp
    src/Utilities.cpp
)

add_library(${This} STATIC ${Sources} ${Headers})
set_target_properties(${This} PROPERTIES
    FOLDER Libraries
)

if(RAFT_EXTRA_DIAGNOSTICS)
    target_compile_definitions(${This} PRIVATE EXTRA_DIAGNOSTICS)
endif(RAFT_EXTRA_DIAGNOSTICS)

target_include_directories(${This} PUBLIC include)

target_link_libraries(${This} PUBLIC
    AsyncData
    Json
    Serialization
    SystemAbstractions
    Timekeeping
)

add_subdirectory(test)
