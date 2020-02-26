#pragma once

/**
 * @file ServerImpl.hpp
 *
 * This module contains the implementation of the Raft::Server class.
 *
 * © 2018-2020 by Richard Walters
 */

#include "InstanceInfo.hpp"
#include "Message.hpp"

#include <algorithm>
#include <AsyncData/MultiProducerSingleConsumerQueue.hpp>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <Raft/LogEntry.hpp>
#include <Raft/ILog.hpp>
#include <Raft/Server.hpp>
#include <random>
#include <stdint.h>
#include <SystemAbstractions/DiagnosticsSender.hpp>
#include <Timekeeping/Scheduler.hpp>
#include <thread>
#include <vector>

namespace Raft {

    /**
     * This contains the private properties of a Server class instance
     * that don't live any longer than the Server class instance itself.
     */
    struct Server::Impl
        : std::enable_shared_from_this< Impl >
    {
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

        bool mobilized = false;

        size_t generation = 0;

        /**
         * This is the next identifier to use for an event subscription.
         */
        int nextEventSubscriberId = 0;

        /**
         * These are the current subscriptions to server events.
         */
        std::map< int, Raft::IServer::EventDelegate > eventSubscribers;

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
        size_t newServerCatchUpIndex = 0;

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
         * This is the function to call whenever a message is received
         * by the server.
         */
        std::function< void() > onReceiveMessageCallback;

        /**
         * This is the time, according to the time keeper, when the server
         * either started or last received a message from the cluster leader.
         */
        double timeOfLastLeaderMessage = 0.0;

        /**
         * This is the scheduler token for the callback that happens
         * when the election timer expires.
         */
        int electionTimeoutToken = 0;

        /**
         * This is the scheduler token for the callback that happens
         * when the server should send out heartbeats.
         */
        int heartbeatTimeoutToken = 0;

        /**
         * This indicates whether the server is a currently a leader,
         * candidate, or follower in the current election term of the cluster.
         */
        Raft::IServer::ElectionState electionState = Raft::IServer::ElectionState::Follower;

        /**
         * This indicates whether or not the leader of the current term is
         * known.
         */
        bool thisTermLeaderAnnounced = false;

        /**
         * This flag indicates whether or not the server is currently
         * processing a message received from the cluster leader.
         */
        bool processingMessageFromLeader = false;

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

        /**
         * This is the number of broadcast time measurements that have
         * been taken since the server started or statistics were reset.
         */
        size_t numBroadcastTimeMeasurements = 0;

        /**
         * This is the next index to set in the broadcast time measurement
         * memory.
         */
        size_t nextBroadcastTimeMeasurementIndex = 0;

        /**
         * This is the sum of all broadcast time measurements that have been
         * made, in microseconds.
         */
        uintmax_t broadcastTimeMeasurementsSum = 0;

        /**
         * This is the minimum measured broadcast time, in microseconds,
         * since the server started or statistics were reset.
         */
        uintmax_t minBroadcastTime = 0;

        /**
         * This is the maximum measured broadcast time, in microseconds,
         * since the server started or statistics were reset.
         */
        uintmax_t maxBroadcastTime = 0;

        /**
         * This holds onto the broadcast time measurements that have been made.
         * The measurements are stored in units of microseconds.
         */
        std::vector< uintmax_t > broadcastTimeMeasurements;

        /**
         * This is the number of measurements of time between receipts of
         * leader messages that have been taken since the server started or
         * statistics were reset.
         */
        size_t numTimeBetweenLeaderMessagesMeasurements = 0;

        /**
         * This is the next index to set in the measurements of time between
         * receipts of leader messages memory.
         */
        size_t nextTimeBetweenLeaderMessagesMeasurementIndex = 0;

        /**
         * This is the sum of all measurements of time between receipts of
         * leader messages that have been made, in microseconds.
         */
        uintmax_t timeBetweenLeaderMessagesMeasurementsSum = 0;

        /**
         * This is the minimum measurement of time between receipts of leader
         * messages, in microseconds, since the server started or statistics
         * were reset.
         */
        uintmax_t minTimeBetweenLeaderMessages = 0;

        /**
         * This is the maximum measurement of time between receipts of leader
         * messages, in microseconds, since the server started or statistics
         * were reset.
         */
        uintmax_t maxTimeBetweenLeaderMessages = 0;

        /**
         * This holds onto the measurements of time between receipts of leader
         * messages that have been made.  The measurements are stored in units
         * of microseconds.
         */
        std::vector< uintmax_t > timeBetweenLeaderMessagesMeasurements;

        /**
         * This is the time, according to the configured time keeper,
         * when the last message was received from the leader of the cluster.
         */
        double timeLastMessageReceivedFromLeader = 0.0;

        /**
         * This is the initial "last index" of the leader, used to determine
         * when the server has "caught up" to the rest of the cluster.
         */
        size_t selfCatchUpIndex = 0;

        /**
         * This flag is set once the server has "caught up" to the rest of the
         * cluster (the commit index has reached the initial "last index" of
         * the leader).
         */
        bool caughtUp = false;

        /**
         * This is the object used to track time for the server
         * and call back functions at specific times.
         */
        std::shared_ptr< Timekeeping::Scheduler > scheduler;

        /**
         * This holds events to be published by the server in its worker
         * thread.
         */
        AsyncData::MultiProducerSingleConsumerQueue<
            std::shared_ptr< Raft::IServer::Event >
        > eventQueue;

        /**
         * This thread publishes any events in the event queue.
         */
        std::thread eventQueueWorker;

        /**
         * This is notified whenever the event queue is no longer empty,
         * and when the event queue worker thread should stop.
         */
        std::condition_variable eventQueueWorkerWakeCondition;

        /**
         * This is used to synchronize access to the event queue worker thread.
         */
        std::mutex eventQueueMutex;

        /**
         * This indicates whether or not the event queue worker thread should
         * stop.
         */
        bool stopEventQueueWorker = false;

        // Methods

        /**
         * This is the default constructor of the class.
         */
        Impl();

        /**
         * Update broadcast time statistics by adding the next measurement,
         * which is the difference between the current time and the given
         * send time.
         *
         * @param[in] sendTime
         *     This is the time the broadcast message was sent, presuming
         *     the response was just received now.
         */
        void MeasureBroadcastTime(double sendTime);

        /**
         * Update time between leader messages statistics by adding the next
         * measurement, which is the difference between the current time and
         * the time when the server last received a message from the leader.
         */
        void MeasureTimeBetweenLeaderMessages();

        /**
         * Return the set of identifiers for the instances currently involved
         * with the cluster.
         *
         * @return
         *     The set of identifiers for the instances currently involved
         *     with the cluster is returned.
         */
        const std::set< int >& GetInstanceIds() const;

        /**
         * Return an indication of whether or not all new servers have "caught
         * up" with the rest of the cluster.
         *
         * @return
         *     An indication of whether or not all new servers have "caught
         *     up" with the rest of the cluster is returned.
         */
        bool HaveNewServersCaughtUp() const;

        /**
         * Set up the initial state for what we track about another server.
         */
        void InitializeInstanceInfo(InstanceInfo& instance);

        /**
         * Set up a callback to trigger a new election if no message is
         * received from the cluster leader within a reasonable time period.
         */
        void ResetElectionTimer();

        /**
         * Set up a callback to trigger sending a heartbeat message to all
         * servers if no journal updates are made within a reasonable time
         * period.
         */
        void ResetHeartbeatTimer();

        void ResetStatistics();

        /**
         * This method queues the given serialized message to be sent later
         * to the instance with the given unique identifier.
         *
         * @param[in] message
         *     This is the message to send.
         *
         * @param[in] instanceNumber
         *     This is the unique identifier of the recipient of the message.
         *
         * @param[in] now
         *     This is the current time, according to the time keeper.
         *
         * @param[in] timeout
         *     This is the amount of time that can elapse without a response
         *     before a retransmission is prompted.
         */
        void QueueSerializedMessageToBeSent(
            const std::string& message,
            int instanceNumber,
            double now,
            double timeout
        );

        /**
         * This method serializes and queues the given message to be sent
         * later to the instance with the given unique identifier.
         *
         * @param[in,out] message
         *     This is the message to send.  The message's sequence number
         *     is set to match the sequence number used for the recipient.
         *
         * @param[in] instanceNumber
         *     This is the unique identifier of the recipient of the message.
         *
         * @param[in] now
         *     This is the current time, according to the time keeper.
         */
        void SerializeAndQueueMessageToBeSent(
            Message& message,
            int instanceNumber,
            double now
        );

        /**
         * This method queues a leadership announcement message to be sent
         * later.
         *
         * @param[in] leaderId
         *     This is the unique identifier of the server which has become
         *     the leader of the cluster.
         *
         * @param[in] term
         *     This is the generation number of the server cluster leadership,
         *     which is incremented whenever a new election is started.
         */
        void QueueLeadershipChangeAnnouncement(
            int leaderId,
            int term
        );

        /**
         * Queues an eleection state change announcement message to be sent
         * later.
         */
        void QueueElectionStateChangeAnnouncement();

        /**
         * Queue a configuration applied announcement message to be sent
         * later.
         *
         * @param[in] newConfiguration
         *     This is the new configuration that was applied.
         */
        void QueueConfigAppliedAnnouncement(
            const ClusterConfiguration& newConfiguration
        );

        /**
         * Queue a configuration committed announcement message to be sent
         * later.
         *
         * @param[in] newConfiguration
         *     This is the new configuration that was committed.
         *
         * @param[in] logIndex
         *     This is the index of the log at the point where the cluster
         *     configuration was committed.
         */
        void QueueConfigCommittedAnnouncement(
            const ClusterConfiguration& newConfiguration,
            size_t logIndex
        );

        /**
         * Queue a "caught up" announcement message to be sent later.
         */
        void QueueCaughtUpAnnouncement();

        /**
         * Queue a snapshot announcement message to be sent later.
         *
         * @param[in] snapshot
         *     This contains a complete copy of the server state, built from
         *     the first log entry up to and including the entry at the
         *     given last included index.
         *
         * @param[in] lastIncludedIndex
         *     This is the index of the last log entry that was used to
         *     assemble the snapshot.
         *
         * @param[in] lastIncludedTerm
         *     This is the term of the last log entry that was used to
         *     assemble the snapshot.
         */
        void QueueSnapshotAnnouncement(
            Json::Value&& snapshot,
            size_t lastIncludedIndex,
            int lastIncludedTerm
        );

        /**
         * Ask the scheduler to clear any already-scheduled callbacks.
         */
        void CancelAllCallbacks();

        /**
         * Ask the scheduler to clear any retransmission callbacks.
         */
        void CancelRetransmissionCallbacks(InstanceInfo& instance);

        /**
         * This method sets the server up as a candidate in the current term
         * and records that it voted for itself and is awaiting votes from all
         * the other servers.
         */
        void StepUpAsCandidate();

        /**
         * This method sends out the first requests for the other servers in
         * the cluster to vote for this server.
         *
         * @param[in] now
         *     This is the current time according to the time keeper.
         */
        void SendInitialVoteRequests(double now);

        /**
         * This method starts a new election for leader of the server cluster.
         *
         * @param[in] now
         *     This is the current time according to the time keeper.
         */
        void StartElection(double now) ;

        /**
         * Queue an AppendEntries message to the server with the given unique
         * identifier, containing all log entries starting with the one at the
         * next index currently recorded for the server.
         *
         * @param[in] instanceId
         *     This is the unique identifier of the server to which to attempt
         *     to replicate log entries.
         */
        void AttemptLogReplication(int instanceId);

        /**
         * This method sends a heartbeat message to all other servers in the
         * server cluster.
         */
        void QueueHeartBeatsToBeSent();

        /**
         * This method sends an AppendEntries message to all other servers in
         * the server cluster.
         *
         * @param[in] now
         *     This is the current time according to the time keeper.
         *
         * @param[in] entries
         *     These are the log entries to include in the message.
         */
        void QueueAppendEntriesToBeSent(
            double now,
            const std::vector< LogEntry >& entries
        );

        /**
         * This method retransmits the last RPC message sent to the given
         * instance if no response has yet been received.
         *
         * @param[in] instanceId
         *     This is the unique identifier of the recipient of the message.
         */
        void QueueRetransmission(int instanceId);

        void AddToEventQueue(std::shared_ptr< Raft::IServer::Event >&& event);

        /**
         * Empty out the event queue, processing each event in order.
         *
         * @param[in] lock
         *     This is the object holding the mutex protecting the shared
         *     properties of the server.
         */
        void ProcessEventQueue(
            std::unique_lock< decltype(eventQueueMutex) >& lock
        );

        /**
         * This method is called to update the current cluster election term.
         *
         * @param[in] newTerm
         *     This is the new term of the cluster.
         */
        void UpdateCurrentTerm(int newTerm);

        /**
         * This method updates the server state to make the server a
         * "follower", as in not seeking election, and not the leader.
         */
        void RevertToFollower();

        /**
         * Set up all the state and mechanisms that are required to be set up
         * or started once the server becomes the leader of the cluster.
         */
        void AssumeLeadership();

        /**
         * Set a single cluster configuration.
         *
         * @param[in] newClusterConfiguration
         *     This is the configuration to set.
         */
        void ApplyConfiguration(const ClusterConfiguration& newClusterConfiguration);

        /**
         * Set a joint cluster configuration.
         *
         * @param[in] newClusterConfiguration
         *     This is the current configuration to set.
         *
         * @param[in] newNextClusterConfiguration
         *     This is the next configuration to set.
         */
        void ApplyConfiguration(
            const ClusterConfiguration& newClusterConfiguration,
            const ClusterConfiguration& newNextClusterConfiguration
        );

        /**
         * Update the last index of the log, processing all log entries
         * between the previous last index and the new last index,
         * possibly rolling back to a previous state.
         *
         * @param[in] newLastIndex
         *     This is the new value to set for the last index.
         */
        void SetLastIndex(size_t newLastIndex);

        /**
         * Determine whether or not there is a command in the log from the
         * given index to the end which, when passed to the given visitor
         * function, returns true.
         *
         * @param[in] visitor
         *     This is function which is used to find a command in the log.
         *
         * @param[in] index
         *     This is the starting index for which to search for the command.
         *
         * @return
         *     An indication of whether or not a command is found in the log
         *     from the given index to the end that causes the given visitor
         *     to return true is returned.
         */
        bool IsCommandApplied(
            std::function< bool(std::shared_ptr< ::Raft::Command > command) > visitor,
            size_t index
        );

        /**
         * Determine whether or not there is a command of the given type
         * anywhere in the log from the given index to the end.
         *
         * @param[in] type
         *     This is the type of command for which to search.
         *
         * @param[in] index
         *     This is the starting index for which to search for the command.
         *
         * @return
         *     An indication of whether or not a command of the given type
         *     is anywhere in the log from the given index to the end
         *     is returned.
         */
        bool IsCommandApplied(
            const std::string& type,
            size_t index
        );

        /**
         * Update the commit index of the log, processing all log entries
         * between the previous commit index and the new commit index.
         *
         * @param[in] newCommitIndex
         *     This is the new value to set for the commit index.
         */
        void AdvanceCommitIndex(size_t newCommitIndex);

        /**
         * Add the given entries to the server log.  Send out AppendEntries
         * messages to the rest of the server cluster in order to replicate the
         * log entries.
         *
         * @param[in] entries
         *     These are the log entries to be added and replicated on all
         *     servers in the cluster.
         */
        void AppendLogEntries(const std::vector< LogEntry >& entries);

        /**
         * If all new servers are caught up now, append a JointConfiguration
         * command to the log in order to start the configuration change
         * process.
         */
        void StartConfigChangeIfNewServersHaveCaughtUp();

        /**
         * Perform any work that is required when the cluster configuration
         * changes.
         */
        void OnSetClusterConfiguration();

        /**
         * This method is called whenever the server receives a request to vote
         * for another server in the cluster.
         *
         * @param[in] message
         *     This is the message received.
         *
         * @param[in] senderInstanceNumber
         *     This is the unique identifier of the server that sent the
         *     message.
         */
        void OnReceiveRequestVote(
            Message&& message,
            int senderInstanceNumber
        );

        /**
         * This method is called whenever the server receives a response to
         * a vote request.
         *
         * @param[in] message
         *     This is the message received.
         *
         * @param[in] senderInstanceNumber
         *     This is the unique identifier of the server that sent the
         *     message.
         */
        void OnReceiveRequestVoteResults(
            Message&& message,
            int senderInstanceNumber
        );

        /**
         * This method is called whenever the server receives an AppendEntries
         * message from the cluster leader.
         *
         * @param[in] message
         *     This is the message received.
         *
         * @param[in] senderInstanceNumber
         *     This is the unique identifier of the server that sent the
         *     message.
         */
        void OnReceiveAppendEntries(
            Message&& message,
            int senderInstanceNumber
        );

        /**
         * Handle the receipt of a response from a follower to an AppendEntries
         * message.
         *
         * @param[in] message
         *     This is the message received.
         *
         * @param[in] senderInstanceNumber
         *     This is the unique identifier of the server that sent the
         *     message.
         */
        void OnReceiveAppendEntriesResults(
            Message&& message,
            int senderInstanceNumber
        );

        /**
         * This method is called whenever the server receives an
         * InstallSnapshot message from the cluster leader.
         *
         * @param[in] message
         *     This is the message received.
         *
         * @param[in] senderInstanceNumber
         *     This is the unique identifier of the server that sent the
         *     message.
         */
        void OnReceiveInstallSnapshot(
            Message&& message,
            int senderInstanceNumber
        );

        /**
         * This method is called whenever the server receives an
         * InstallSnapshotResults message from the cluster leader.
         *
         * @param[in] message
         *     This is the message received.
         *
         * @param[in] senderInstanceNumber
         *     This is the unique identifier of the server that sent the
         *     message.
         */
        void OnReceiveInstallSnapshotResults(
            Message&& message,
            int senderInstanceNumber
        );

        /**
         * This runs in a thread which publishes any events pushed into
         * the event queue.
         */
        void EventQueueWorker();
    };

}
