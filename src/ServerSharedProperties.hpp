#ifndef RAFT_SERVER_SHARED_PROPERTIES_HPP
#define RAFT_SERVER_SHARED_PROPERTIES_HPP

/**
 * @file ServerSharedProperties.hpp
 *
 * This module contains the declaration of the Raft::ServerSharedProperties
 * structure.
 *
 * © 2019 by Richard Walters
 */

#include "ConfigCommittedAnnouncement.hpp"
#include "ElectionStateChangeAnnouncement.hpp"
#include "InstanceInfo.hpp"
#include "LeadershipAnnouncement.hpp"
#include "Message.hpp"
#include "MessageToBeSent.hpp"

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
#include <SystemAbstractions/DiagnosticsSender.hpp>
#include <thread>

namespace Raft {

    /**
     * This contains the private properties of a Server class instance
     * that may live longer than the Server class instance itself.
     */
    struct ServerSharedProperties {
        // Properties

        /**
         * This is a helper object used to generate and publish
         * diagnostic messages.
         */
        SystemAbstractions::DiagnosticsSender diagnosticsSender;

        /**
         * This is used to synchronize access to the properties below.
         */
        std::recursive_mutex mutex;

        /**
         * This holds all configuration items for the server cluster.
         */
        Raft::ClusterConfiguration clusterConfiguration;

        /**
         * This holds all configuration items to be set for the server once
         * the current configuration transition is complete.
         */
        Raft::ClusterConfiguration nextClusterConfiguration;

        /**
         * This indicates whether or not the cluster is transitioning from the
         * current configuration to the next configuration.
         */
        std::unique_ptr< Raft::ClusterConfiguration > jointConfiguration;

        /**
         * This indicates whether or not a cluster configuration change is
         * about to happen, once all new servers have "caught up" with the rest
         * of the cluster.
         */
        bool configChangePending = false;

        /**
         * If the server is waiting for all new servers to have "caught up"
         * with the rest of the cluster, this is the minimum matchIndex
         * required to consider a server to have "caught up".
         */
        size_t catchUpIndex = 0;

        /**
         * This holds all configuration items for the server instance.
         */
        Raft::IServer::ServerConfiguration serverConfiguration;

        /**
         * This is a cache of the server's persistent state variables.
         */
        Raft::IPersistentState::Variables persistentStateCache;

        /**
         * This is a standard C++ Mersenne Twister pseudo-random number
         * generator, used to pick election timeouts.
         */
        std::mt19937 rng;

        /**
         * This holds messages to be sent to other servers by the worker
         * thread.
         */
        std::queue< MessageToBeSent > messagesToBeSent;

        /**
         * This holds leadership announcements to be sent by the worker thread.
         */
        std::queue< LeadershipAnnouncement > leadershipAnnouncementsToBeSent;

        /**
         * This holds election state change announcements to be sent by the
         * worker thread.
         */
        std::queue< ElectionStateChangeAnnouncement > electionStateChangeAnnouncementsToBeSent;

        /**
         * This holds cluster configuration applied announcements to be sent by
         * the worker thread.
         */
        std::queue< Raft::ClusterConfiguration > configAppliedAnnouncementsToBeSent;

        /**
         * This holds cluster configuration committed announcements to be sent
         * by the worker thread.
         */
        std::queue< ConfigCommittedAnnouncement > configCommittedAnnouncementsToBeSent;

        /**
         * If this is not nullptr, then the worker thread should set the result
         * once it executes a full loop.
         */
        std::shared_ptr< std::promise< void > > workerLoopCompletion;

        /**
         * This is the maximum amount of time to wait, between messages from
         * the cluster leader, before calling a new election.
         */
        double currentElectionTimeout = 0.0;

        /**
         * This is the time, according to the time keeper, when the server
         * either started or last received a message from the cluster leader.
         */
        double timeOfLastLeaderMessage = 0.0;

        /**
         * This indicates whether the server is a currently a leader,
         * candidate, or follower in the current election term of the cluster.
         */
        Raft::IServer::ElectionState electionState = Raft::IServer::ElectionState::Follower;

        /**
         * This indicates whether or not the server has sent out at least
         * one set of heartbeats since it last assumed leadership.
         */
        bool sentHeartBeats = false;

        /**
         * This indicates whether or not the leader of the current term is
         * known.
         */
        bool thisTermLeaderAnnounced = false;

        /**
         * This is the unique identifier of the cluster leader, if known.
         */
        int leaderId = 0;

        /**
         * During an election, this is the number of votes we have received
         * for ourselves amongst the servers in the current configuration.
         */
        size_t votesForUsCurrentConfig = 0;

        /**
         * During an election, this is the number of votes we have received
         * for ourselves amongst the servers in the next configuration.
         */
        size_t votesForUsNextConfig = 0;

        /**
         * This indicates whether or not the server is allowed to run for
         * election to be leader of the cluster, or vote for another server to
         * be leader.
         */
        bool isVotingMember = true;

        /**
         * This holds information this server tracks about the other servers.
         */
        std::map< int, InstanceInfo > instances;

        /**
         * This is the index of the last log entry known to have been appended
         * to a majority of servers in the cluster.
         */
        size_t commitIndex = 0;

        /**
         * This is the index of the last entry appended to the log.
         */
        size_t lastIndex = 0;

        /**
         * This is the object which is responsible for keeping
         * the actual log and making it persistent.
         */
        std::shared_ptr< Raft::ILog > logKeeper;

        /**
         * This is the object which is responsible for keeping
         * the server state variables which need to be persistent.
         */
        std::shared_ptr< Raft::IPersistentState > persistentStateKeeper;

        // Methods

        ServerSharedProperties();
    };

}

#endif /* RAFT_SERVER_SHARED_PROPERTIES_HPP */
