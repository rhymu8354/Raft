# CMakeLists.txt for Raft
#
# © 2018 by Richard Walters

cmake_minimum_required(VERSION 3.8)
set(This Raft)

set(Headers
    include/Raft/ClusterConfiguration.hpp
    include/Raft/Command.hpp
    include/Raft/ILog.hpp
    include/Raft/IPersistentState.hpp
    include/Raft/IServer.hpp
    include/Raft/LogEntry.hpp
    include/Raft/Server.hpp
    include/Raft/TimeKeeper.hpp
    src/ConfigCommittedAnnouncement.hpp
    src/ElectionStateChangeAnnouncement.hpp
    src/InstanceInfo.hpp
    src/LeadershipAnnouncement.hpp
    src/Message.hpp
    src/MessageToBeSent.hpp
    src/ServerImpl.hpp
    src/ServerSharedProperties.hpp
    src/SnapshotAnnouncement.hpp
    src/Utilities.hpp
)

set(Sources
    src/LogEntry.cpp
    src/Message.cpp
    src/Server.cpp
    src/ServerImpl.cpp
    src/ServerSharedProperties.cpp
    src/Utilities.cpp
)

add_library(${This} STATIC ${Sources} ${Headers})
set_target_properties(${This} PROPERTIES
    FOLDER Libraries
)

target_include_directories(${This} PUBLIC include)

target_link_libraries(${This} PUBLIC
    Json
    SystemAbstractions
)

add_subdirectory(test)
