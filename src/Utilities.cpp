/**
 * @file Utilities.cpp
 *
 * This module contains the implementation of free functions used by other
 * parts of the library implementation.
 *
 * © 2019 by Richard Walters
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
#include <Raft/TimeKeeper.hpp>
#include <random>
#include <sstream>
#include <SystemAbstractions/CryptoRandom.hpp>
#include <SystemAbstractions/DiagnosticsSender.hpp>
#include <thread>
#include <time.h>

namespace Raft {

    void PrintTo(
        const Raft::IServer::ElectionState& electionState,
        std::ostream* os
    ) {
        switch (electionState) {
            case Raft::IServer::ElectionState::Follower: {
                *os << "Follower";
            } break;
            case Raft::IServer::ElectionState::Candidate: {
                *os << "Candidate";
            } break;
            case Raft::IServer::ElectionState::Leader: {
                *os << "Leader";
            } break;
            default: {
                *os << "???";
            };
        }
    }

    std::string ElectionStateToString(Raft::Server::ElectionState electionState) {
        switch (electionState) {
            case Raft::Server::ElectionState::Follower: return "Follower";
            case Raft::Server::ElectionState::Candidate: return "Candidate";
            case Raft::Server::ElectionState::Leader: return "Leader";
            default: return "???";
        }
    }

    void SendMessages(
        Raft::IServer::SendMessageDelegate sendMessageDelegate,
        std::queue< MessageToBeSent >&& messagesToBeSent
    ) {
        while (!messagesToBeSent.empty()) {
            const auto& messageToBeSent = messagesToBeSent.front();
            sendMessageDelegate(
                messageToBeSent.message,
                messageToBeSent.receiverInstanceNumber
            );
            messagesToBeSent.pop();
        }
    }

    void SendLeadershipAnnouncements(
        Raft::IServer::LeadershipChangeDelegate leadershipChangeDelegate,
        std::queue< LeadershipAnnouncement >&& leadershipAnnouncementsToBeSent
    ) {
        while (!leadershipAnnouncementsToBeSent.empty()) {
            const auto& leadershipAnnouncementToBeSent = leadershipAnnouncementsToBeSent.front();
            leadershipChangeDelegate(
                leadershipAnnouncementToBeSent.leaderId,
                leadershipAnnouncementToBeSent.term
            );
            leadershipAnnouncementsToBeSent.pop();
        }
    }

    void SendElectionStateChangeAnnouncements(
        Raft::IServer::ElectionStateChangeDelegate electionStateChangeDelegate,
        std::queue< ElectionStateChangeAnnouncement >&& electionStateChangeAnnouncementsToBeSent
    ) {
        while (!electionStateChangeAnnouncementsToBeSent.empty()) {
            const auto& electionStateChangeAnnouncementToBeSent = electionStateChangeAnnouncementsToBeSent.front();
            electionStateChangeDelegate(
                electionStateChangeAnnouncementToBeSent.term,
                electionStateChangeAnnouncementToBeSent.electionState,
                electionStateChangeAnnouncementToBeSent.didVote,
                electionStateChangeAnnouncementToBeSent.votedFor
            );
            electionStateChangeAnnouncementsToBeSent.pop();
        }
    }

    void SendConfigAppliedAnnouncements(
        Raft::IServer::ApplyConfigurationDelegate applyConfigurationDelegate,
        std::queue< Raft::ClusterConfiguration >&& configAppliedAnnouncementsToBeSent
    ) {
        while (!configAppliedAnnouncementsToBeSent.empty()) {
            const auto& configAppliedAnnouncementToBeSent = configAppliedAnnouncementsToBeSent.front();
            applyConfigurationDelegate(
                configAppliedAnnouncementToBeSent
            );
            configAppliedAnnouncementsToBeSent.pop();
        }
    }

    void SendConfigCommittedAnnouncements(
        Raft::IServer::CommitConfigurationDelegate commitConfigurationDelegate,
        std::queue< ConfigCommittedAnnouncement >&& configCommittedAnnouncementsToBeSent
    ) {
        while (!configCommittedAnnouncementsToBeSent.empty()) {
            const auto& snapshotAnnouncementToBeSent = configCommittedAnnouncementsToBeSent.front();
            commitConfigurationDelegate(
                snapshotAnnouncementToBeSent.newConfig,
                snapshotAnnouncementToBeSent.logIndex
            );
            configCommittedAnnouncementsToBeSent.pop();
        }
    }

    void SendSnapshotAnnouncements(
        Raft::IServer::SnapshotDelegate snapshotDelegate,
        std::queue< SnapshotAnnouncement >&& snapshotAnnouncementsToBeSent
    ) {
        while (!snapshotAnnouncementsToBeSent.empty()) {
            const auto& snapshotAnnouncementToBeSent = snapshotAnnouncementsToBeSent.front();
            snapshotDelegate(
                snapshotAnnouncementToBeSent.snapshot,
                snapshotAnnouncementToBeSent.lastIncludedIndex,
                snapshotAnnouncementToBeSent.lastIncludedTerm
            );
            snapshotAnnouncementsToBeSent.pop();
        }
    }

}
