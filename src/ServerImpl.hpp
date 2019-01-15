#ifndef RAFT_SERVER_IMPL_HPP
#define RAFT_SERVER_IMPL_HPP

/**
 * @file ServerImpl.hpp
 *
 * This module contains the implementation of the Raft::Server class.
 *
 * © 2018 by Richard Walters
 */

#include "InstanceInfo.hpp"
#include "Message.hpp"
#include "ServerSharedProperties.hpp"

#include <future>
#include <Raft/LogEntry.hpp>
#include <Raft/ILog.hpp>
#include <Raft/Server.hpp>
#include <Raft/TimeKeeper.hpp>
#include <thread>

namespace Raft {

    /**
     * This contains the private properties of a Server class instance
     * that don't live any longer than the Server class instance itself.
     */
    struct Server::Impl {
        // Properties

        /**
         * This holds any properties of the Server that might live longer
         * than the Server itself (e.g. due to being captured in
         * callbacks).
         */
        std::shared_ptr< ServerSharedProperties > shared = std::make_shared< ServerSharedProperties >();

        /**
         * This is the object used to track time for the server.
         */
        std::shared_ptr< TimeKeeper > timeKeeper;

        /**
         * This is the delegate to be called whenever the server
         * wants to send a message to another server in the cluster.
         */
        SendMessageDelegate sendMessageDelegate;

        /**
         * This is the delegate to be called later whenever a leadership change
         * occurs in the server cluster.
         */
        LeadershipChangeDelegate leadershipChangeDelegate;

        /**
         * This is the delegate to be called later whenever a single
         * cluster configuration is applied by the server.
         */
        ApplyConfigurationDelegate applyConfigurationDelegate;

        /**
         * This is the delegate to be called later whenever a single
         * cluster configuration is applied by the server.
         */
        CommitConfigurationDelegate commitConfigurationDelegate;

        /**
         * This thread performs any background tasks required of the
         * server, such as starting an election if no message is received
         * from the cluster leader before the next timeout.
         */
        std::thread worker;

        /**
         * This is set when the worker thread should stop.
         */
        std::promise< void > stopWorker;

        /**
         * This is notified whenever the thread is asked to stop or to
         * wake up.
         */
        std::condition_variable_any workerAskedToStopOrWakeUp;

        // Methods

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
         * This method is called whenever a message is received from the
         * cluster leader, or when the server starts an election, or starts up
         * initially.  It samples the current time from the time keeper and
         * stores it in the timeOfLastLeaderMessage shared property.  It also
         * picks a new election timeout.
         */
        void ResetElectionTimer();

        /**
         * This method queues the given message to be sent later to the
         * instance with the given unique identifier.
         *
         * @param[in] message
         *     This is the message to send.
         *
         * @param[in] instanceNumber
         *     This is the unique identifier of the recipient of the message.
         *
         * @param[in] now
         *     This is the current time, according to the time keeper.
         */
        void QueueMessageToBeSent(
            std::string message,
            int instanceNumber,
            double now
        );

        /**
         * This method queues the given message to be sent later to the
         * instance with the given unique identifier.
         *
         * @param[in] message
         *     This is the message to send.
         *
         * @param[in] instanceNumber
         *     This is the unique identifier of the recipient of the message.
         *
         * @param[in] now
         *     This is the current time, according to the time keeper.
         */
        void QueueMessageToBeSent(
            const Message& message,
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
         * Reset state variables involved in the retransmission process.
         */
        void ResetRetransmissionState();

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
         *
         * @param[in] now
         *     This is the current time according to the time keeper.
         */
        void QueueHeartBeatsToBeSent(double now);

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
         * This method retransmits any RPC messages for which no response has
         * yet been received.
         *
         * @param[in] now
         *     This is the current time according to the time keeper.
         */
        void QueueRetransmissionsToBeSent(double now);

        /**
         * This method is called in order to send any messages queued up to be
         * sent to other servers.
         *
         * @param[in] lock
         *     This is the object holding the mutex protecting the shared
         *     properties of the server.
         */
        void SendQueuedMessages(
            std::unique_lock< decltype(shared->mutex) >& lock
        );

        /**
         * This method is called in order to send any queued leadership
         * announcements.
         *
         * @param[in] lock
         *     This is the object holding the mutex protecting the shared
         *     properties of the server.
         */
        void SendQueuedLeadershipAnnouncements(
            std::unique_lock< decltype(shared->mutex) >& lock
        );

        /**
         * Send any queued cluster configuration applied announcements.
         *
         * @param[in] lock
         *     This is the object holding the mutex protecting the shared
         *     properties of the server.
         */
        void SendQueuedConfigAppliedAnnouncements(
            std::unique_lock< decltype(shared->mutex) >& lock
        );

        /**
         * Send any queued cluster configuration committed announcements.
         *
         * @param[in] lock
         *     This is the object holding the mutex protecting the shared
         *     properties of the server.
         */
        void SendQueuedConfigCommittedAnnouncements(
            std::unique_lock< decltype(shared->mutex) >& lock
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
         * @param[in] clusterConfiguration
         *     This is the configuration to set.
         */
        void ApplyConfiguration(const ClusterConfiguration& clusterConfiguration);

        /**
         * Set a joint cluster configuration.
         *
         * @param[in] clusterConfiguration
         *     This is the current configuration to set.
         *
         * @param[in] nextClusterConfiguration
         *     This is the next configuration to set.
         */
        void ApplyConfiguration(
            const ClusterConfiguration& clusterConfiguration,
            const ClusterConfiguration& nextClusterConfiguration
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
         *     This contains the details of the message received.
         *
         * @param[in] senderInstanceNumber
         *     This is the unique identifier of the server that sent the
         *     message.
         */
        void OnReceiveRequestVote(
            const Message::RequestVoteDetails& messageDetails,
            int senderInstanceNumber
        );

        /**
         * This method is called whenever the server receives a response to
         * a vote request.
         *
         * @param[in] message
         *     This contains the details of the message received.
         *
         * @param[in] senderInstanceNumber
         *     This is the unique identifier of the server that sent the
         *     message.
         */
        void OnReceiveRequestVoteResults(
            const Message::RequestVoteResultsDetails& messageDetails,
            int senderInstanceNumber
        );

        /**
         * This method is called whenever the server receives an AppendEntries
         * message from the cluster leader.
         *
         * @param[in] message
         *     This contains the details of the message received.
         *
         * @param[in] entries
         *     These are the log entries that came with the message.
         *
         * @param[in] senderInstanceNumber
         *     This is the unique identifier of the server that sent the
         *     message.
         */
        void OnReceiveAppendEntries(
            const Message::AppendEntriesDetails& messageDetails,
            std::vector< LogEntry >&& entries,
            int senderInstanceNumber
        );

        /**
         * Handle the receipt of a response from a follower to an AppendEntries
         * message.
         *
         * @param[in] message
         *     This contains the details of the message received.
         *
         * @param[in] senderInstanceNumber
         *     This is the unique identifier of the server that sent the
         *     message.
         */
        void OnReceiveAppendEntriesResults(
            const Message::AppendEntriesResultsDetails& messageDetails,
            int senderInstanceNumber
        );

        /**
         * This method is used by the worker thread to suspend itself until
         * more work needs to be done.
         *
         * @param[in,out] lock
         *     This is the object used to manage the shared properties mutex in
         *     the worker thread.
         *
         * @param[in,out] workerAskedToStop
         *     This is the receiving end of the promise made by the overall
         *     class to tell the worker thread to stop.
         */
        void WaitForWork(
            std::unique_lock< decltype(shared->mutex) >& lock,
            std::future< void >& workerAskedToStop
        );

        /**
         * This is the logic to perform once per worker thread loop.
         *
         * @param[in,out] lock
         *     This is the object used to manage the shared properties mutex in
         *     the worker thread.
         */
        void WorkerLoopBody(
            std::unique_lock< decltype(shared->mutex) >& lock
        );

        /**
         * This runs in a thread and performs any background tasks required of
         * the Server, such as starting an election if no message is received
         * from the cluster leader before the next timeout.
         */
        void Worker();
    };

}

#endif /* RAFT_SERVER_IMPL_HPP */
