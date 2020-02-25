/**
 * @file Utilities.cpp
 *
 * This module contains the implementation of free functions used by other
 * parts of the library implementation.
 *
 * © 2019-2020 by Richard Walters
 */

#include "Utilities.hpp"

#include <algorithm>
#include <future>
#include <map>
#include <mutex>
#include <queue>
#include <Raft/LogEntry.hpp>
#include <Raft/ILog.hpp>
#include <Raft/Server.hpp>
#include <random>
#include <sstream>
#include <SystemAbstractions/CryptoRandom.hpp>
#include <SystemAbstractions/DiagnosticsSender.hpp>
#include <thread>
#include <time.h>

namespace Raft {

    void PrintTo(
        const IServer::ElectionState& electionState,
        std::ostream* os
    ) {
        switch (electionState) {
            case IServer::ElectionState::Follower: {
                *os << "Follower";
            } break;
            case IServer::ElectionState::Candidate: {
                *os << "Candidate";
            } break;
            case IServer::ElectionState::Leader: {
                *os << "Leader";
            } break;
            default: {
                *os << "???";
            };
        }
    }

    std::string ElectionStateToString(IServer::ElectionState electionState) {
        switch (electionState) {
            case IServer::ElectionState::Follower: return "Follower";
            case IServer::ElectionState::Candidate: return "Candidate";
            case IServer::ElectionState::Leader: return "Leader";
            default: return "???";
        }
    }

}
