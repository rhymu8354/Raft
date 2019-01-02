/**
 * @file Server.cpp
 *
 * This module contains the implementation of the Raft::Server class.
 *
 * © 2018 by Richard Walters
 */

#include "Message.hpp"

#include <algorithm>
#include <future>
#include <map>
#include <mutex>
#include <queue>
#include <Raft/ILog.hpp>
#include <Raft/Server.hpp>
#include <Raft/TimeKeeper.hpp>
#include <random>
#include <SystemAbstractions/CryptoRandom.hpp>
#include <SystemAbstractions/DiagnosticsSender.hpp>
#include <thread>
#include <time.h>

namespace {

    /**
     * This holds information that one server holds about another server.
     */
    struct InstanceInfo {
        /**
         * This indicates whether or not we're awaiting a response to the
         * last RPC call message sent to this instance.
         */
        bool awaitingResponse = false;

        /**
         * This is the time, according to the time keeper, that a request was
         * last sent to the instance.
         */
        double timeLastRequestSent = 0.0;

        /**
         * This is the last request sent to the instance.
         */
        std::string lastRequest;

        /**
         * This is the index of the next log entry to send to this server.
         */
        size_t nextIndex = 0;

        /**
         * This is the index of the highest log entry known to be replicated
         * on this server.
         */
        size_t matchIndex = 0;
    };

    /**
     * This holds information used to store a message to be sent, and later to
     * send the message.
     */
    struct MessageToBeSent {
        /**
         * This is the message to be sent.
         */
        std::string message;

        /**
         * This is the unique identifier of the server to which to send the
         * message.
         */
        int receiverInstanceNumber = 0;
    };

    /**
     * This holds information used to store a leadership announcement to be
     * sent later.
     */
    struct LeadershipAnnouncementToBeSent {
        /**
         * This is the unique identifier of the server which has become
         * the leader of the cluster.
         */
        int leaderId = 0;

        /**
         * This is the generation number of the server cluster leadership,
         * which is incremented whenever a new election is started.
         */
        int term = 0;
    };

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
        std::mutex mutex;

        /**
         * This holds all configuration items for the server cluster.
         */
        Raft::ClusterConfiguration clusterConfiguration;

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
        std::queue< LeadershipAnnouncementToBeSent > leadershipAnnouncementsToBeSent;

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
         * This indicates whether or not the leader of the current term is
         * known.
         */
        bool thisTermLeaderAnnounced = false;

        /**
         * During an election, this is the number of votes we have received
         * for ourselves.
         */
        size_t votesForUs = 0;

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

        ServerSharedProperties()
            : diagnosticsSender("Raft::Server")
        {
        }
    };

    /**
     * Return a human-readable string representation of the given server
     * election state.
     *
     * @param[in] electionState
     *     This is the election state to turn into a string.
     *
     * @return
     *     A human-readable string representation of the given server
     *     election state is returned.
     */
    std::string ElectionStateToString(Raft::Server::ElectionState electionState) {
        switch (electionState) {
            case Raft::Server::ElectionState::Follower: return "Follower";
            case Raft::Server::ElectionState::Candidate: return "Candidate";
            case Raft::Server::ElectionState::Leader: return "Leader";
            default: return "???";
        }
    }

    /**
     * This function sends the given messages to other servers, using the given
     * delegate.
     *
     * @param[in] sendMessageDelegate
     *     This is the delegate to use to send messages to other servers.
     *
     * @param[in,out] messagesToBeSent
     *     This holds the messages to be sent, and is consumed by the function.
     */
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

    /**
     * This function sends the given leadership announcements, using the given
     * delegate.
     *
     * @param[in] leadershipChangeDelegate
     *     This is the delegate to use to send leadership announcements.
     *
     * @param[in,out] leadershipAnnouncementsToBeSent
     *     This holds the leadership announcements to be sent, and is consumed
     *     by the function.
     */
    void SendLeadershipAnnouncements(
        Raft::IServer::LeadershipChangeDelegate leadershipChangeDelegate,
        std::queue< LeadershipAnnouncementToBeSent >&& leadershipAnnouncementsToBeSent
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

}

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
        std::condition_variable workerAskedToStopOrWakeUp;

        // Methods

        /**
         * This method is called whenever a message is received from the
         * cluster leader, or when the server starts an election, or starts up
         * initially.  It samples the current time from the time keeper and
         * stores it in the timeOfLastLeaderMessage shared property.  It also
         * picks a new election timeout.
         */
        void ResetElectionTimer() {
            shared->timeOfLastLeaderMessage = timeKeeper->GetCurrentTime();
            shared->currentElectionTimeout = std::uniform_real_distribution<>(
                shared->serverConfiguration.minimumElectionTimeout,
                shared->serverConfiguration.maximumElectionTimeout
            )(shared->rng);
        }

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
        ) {
            auto& instance = shared->instances[instanceNumber];
            instance.timeLastRequestSent = now;
            instance.lastRequest = message;
            MessageToBeSent messageToBeSent;
            messageToBeSent.message = std::move(message);
            messageToBeSent.receiverInstanceNumber = instanceNumber;
            shared->messagesToBeSent.push(std::move(messageToBeSent));
            workerAskedToStopOrWakeUp.notify_one();
        }

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
        ) {
            if (
                (message.type == Message::Type::RequestVote)
                || (message.type == Message::Type::AppendEntries)
            ) {
                auto& instance = shared->instances[instanceNumber];
                instance.awaitingResponse = true;
            }
            QueueMessageToBeSent(message.Serialize(), instanceNumber, now);
        }

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
        ) {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                3,
                "Server %u is now the leader in term %u",
                leaderId,
                term
            );
            LeadershipAnnouncementToBeSent leadershipAnnouncementToBeSent;
            leadershipAnnouncementToBeSent.leaderId = leaderId;
            leadershipAnnouncementToBeSent.term = term;
            shared->leadershipAnnouncementsToBeSent.push(std::move(leadershipAnnouncementToBeSent));
            workerAskedToStopOrWakeUp.notify_one();
        }

        /**
         * This method sets the server up as a candidate in the current term
         * and records that it voted for itself and is awaiting votes from all
         * the other servers.
         */
        void StepUpAsCandidate() {
            shared->electionState = IServer::ElectionState::Candidate;
            shared->persistentStateCache.votedThisTerm = true;
            shared->persistentStateCache.votedFor = shared->serverConfiguration.selfInstanceId;
            shared->persistentStateKeeper->Save(shared->persistentStateCache);
            shared->votesForUs = 1;
            for (auto& instanceEntry: shared->instances) {
                instanceEntry.second.awaitingResponse = false;
            }
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                2,
                "Timeout -- starting new election (term %u)",
                shared->persistentStateCache.currentTerm
            );
        }

        /**
         * This method sends out the first requests for the other servers in
         * the cluster to vote for this server.
         *
         * @param[in] now
         *     This is the current time according to the time keeper.
         */
        void SendInitialVoteRequests(double now) {
            Message message;
            message.type = Message::Type::RequestVote;
            message.requestVote.candidateId = shared->serverConfiguration.selfInstanceId;
            message.requestVote.term = shared->persistentStateCache.currentTerm;
            message.requestVote.lastLogIndex = shared->lastIndex;
            if (shared->lastIndex > 0) {
                message.requestVote.lastLogTerm = shared->logKeeper->operator[](shared->lastIndex).term;
            } else {
                message.requestVote.lastLogTerm = 0;
            }
            for (auto instanceNumber: shared->clusterConfiguration.instanceIds) {
                if (instanceNumber == shared->serverConfiguration.selfInstanceId) {
                    continue;
                }
                auto& instance = shared->instances[instanceNumber];
                QueueMessageToBeSent(message, instanceNumber, now);
            }
            shared->timeOfLastLeaderMessage = timeKeeper->GetCurrentTime();
        }

        /**
         * This method starts a new election for leader of the server cluster.
         *
         * @param[in] now
         *     This is the current time according to the time keeper.
         */
        void StartElection(double now) {
            UpdateCurrentTerm(shared->persistentStateCache.currentTerm + 1);
            StepUpAsCandidate();
            SendInitialVoteRequests(now);
        }

        /**
         * Queue an AppendEntries message to the server with the given unique
         * identifier, containing all log entries starting with the one at the
         * next index currently recorded for the server.
         *
         * @param[in] instanceId
         *     This is the unique identifier of the server to which to attempt
         *     to replicate log entries.
         */
        void AttemptLogReplication(int instanceId) {
            auto& instance = shared->instances[instanceId];
            Message message;
            message.type = Message::Type::AppendEntries;
            message.appendEntries.term = shared->persistentStateCache.currentTerm;
            message.appendEntries.leaderCommit = shared->commitIndex;
            message.appendEntries.prevLogIndex = instance.nextIndex - 1;
            if (message.appendEntries.prevLogIndex == 0) {
                message.appendEntries.prevLogTerm = 0;
            } else {
                message.appendEntries.prevLogTerm = shared->logKeeper->operator[](message.appendEntries.prevLogIndex).term;
            }
            for (size_t i = instance.nextIndex; i <= shared->lastIndex; ++i) {
                message.log.push_back(shared->logKeeper->operator[](i));
            }
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                0,
                "Sending log entries (%zu entries starting at %zu, term %u)",
                message.log.size(),
                instance.nextIndex,
                shared->persistentStateCache.currentTerm
            );
            QueueMessageToBeSent(
                message,
                instanceId,
                timeKeeper->GetCurrentTime()
            );
        }

        /**
         * This method sends a heartbeat message to all other servers in the
         * server cluster.
         *
         * @param[in] now
         *     This is the current time according to the time keeper.
         */
        void QueueHeartBeatsToBeSent(double now) {
            Message message;
            message.type = Message::Type::AppendEntries;
            message.appendEntries.term = shared->persistentStateCache.currentTerm;
            message.appendEntries.leaderCommit = shared->commitIndex;
            message.appendEntries.prevLogIndex = shared->lastIndex;
            if (shared->lastIndex == 0) {
                message.appendEntries.prevLogTerm = 0;
            } else {
                message.appendEntries.prevLogTerm = shared->logKeeper->operator[](shared->lastIndex).term;
            }
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                0,
                "Sending heartbeat (term %u)",
                shared->persistentStateCache.currentTerm
            );
            for (auto instanceNumber: shared->clusterConfiguration.instanceIds) {
                auto& instance = shared->instances[instanceNumber];
                if (
                    (instanceNumber == shared->serverConfiguration.selfInstanceId)
                    || instance.awaitingResponse
                ) {
                    continue;
                }
                QueueMessageToBeSent(message, instanceNumber, now);
            }
            shared->timeOfLastLeaderMessage = now;
        }

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
        ) {
            Message message;
            message.type = Message::Type::AppendEntries;
            message.appendEntries.term = shared->persistentStateCache.currentTerm;
            message.appendEntries.leaderCommit = shared->commitIndex;
            message.appendEntries.prevLogIndex = shared->lastIndex - entries.size();
            if (message.appendEntries.prevLogIndex == 0) {
                message.appendEntries.prevLogTerm = 0;
            } else {
                message.appendEntries.prevLogTerm = shared->logKeeper->operator[](message.appendEntries.prevLogIndex).term;
            }
            message.log = entries;
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                0,
                "Sending log entries (%zu entries starting at %zu, term %u)",
                entries.size(),
                message.appendEntries.prevLogIndex + 1,
                shared->persistentStateCache.currentTerm
            );
            for (auto instanceNumber: shared->clusterConfiguration.instanceIds) {
                auto& instance = shared->instances[instanceNumber];
                if (
                    (instanceNumber == shared->serverConfiguration.selfInstanceId)
                    || instance.awaitingResponse
                ) {
                    continue;
                }
                QueueMessageToBeSent(message, instanceNumber, now);
            }
            shared->timeOfLastLeaderMessage = timeKeeper->GetCurrentTime();
        }

        /**
         * This method retransmits any RPC messages for which no response has
         * yet been received.
         *
         * @param[in] now
         *     This is the current time according to the time keeper.
         */
        void QueueRetransmissionsToBeSent(double now) {
            for (auto& instanceEntry: shared->instances) {
                if (
                    instanceEntry.second.awaitingResponse
                    && (now - instanceEntry.second.timeLastRequestSent >= shared->serverConfiguration.rpcTimeout)
                ) {
                    QueueMessageToBeSent(
                        instanceEntry.second.lastRequest,
                        instanceEntry.first,
                        now
                    );
                }
            }
        }

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
        ) {
            decltype(shared->messagesToBeSent) messagesToBeSent;
            messagesToBeSent.swap(shared->messagesToBeSent);
            auto sendMessageDelegateCopy = sendMessageDelegate;
            lock.unlock();
            SendMessages(
                sendMessageDelegateCopy,
                std::move(messagesToBeSent)
            );
            lock.lock();
        }

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
        ) {
            decltype(shared->leadershipAnnouncementsToBeSent) leadershipAnnouncementsToBeSent;
            leadershipAnnouncementsToBeSent.swap(shared->leadershipAnnouncementsToBeSent);
            if (leadershipChangeDelegate == nullptr) {
                return;
            }
            auto leadershipChangeDelegateCopy = leadershipChangeDelegate;
            lock.unlock();
            SendLeadershipAnnouncements(
                leadershipChangeDelegateCopy,
                std::move(leadershipAnnouncementsToBeSent)
            );
            lock.lock();
        }

        /**
         * This method is called to update the current cluster election term.
         *
         * @param[in] newTerm
         *     This is the new term of the cluster.
         */
        void UpdateCurrentTerm(int newTerm) {
            if (shared->persistentStateCache.currentTerm < newTerm) {
                shared->thisTermLeaderAnnounced = false;
            }
            shared->persistentStateCache.currentTerm = newTerm;
            shared->persistentStateKeeper->Save(shared->persistentStateCache);
        }

        /**
         * This method updates the server state to make the server a
         * "follower", as in not seeking election, and not the leader.
         */
        void RevertToFollower() {
            for (auto& instanceEntry: shared->instances) {
                instanceEntry.second.awaitingResponse = false;
            }
            shared->electionState = IServer::ElectionState::Follower;
            ResetElectionTimer();
        }

        /**
         * Set up all the state and mechanisms that are required to be set up
         * or started once the server becomes the leader of the cluster.
         */
        void AssumeLeadership() {
            shared->electionState = IServer::ElectionState::Leader;
            shared->diagnosticsSender.SendDiagnosticInformationString(
                3,
                "Received majority vote -- assuming leadership"
            );
            QueueLeadershipChangeAnnouncement(
                shared->serverConfiguration.selfInstanceId,
                shared->persistentStateCache.currentTerm
            );
            for (auto& instanceEntry: shared->instances) {
                instanceEntry.second.nextIndex = shared->lastIndex + 1;
                instanceEntry.second.matchIndex = 0;
            }
        }

        /**
         * Update the last index of the log, processing all log entries
         * between the previous last index and the new last index,
         * possibly rolling back to a previous state.
         *
         * @param[in] newLastIndex
         *     This is the new value to set for the last index.
         */
        void SetLastIndex(size_t newLastIndex) {
            const auto firstEntryToProcess = shared->lastIndex + 1;
            shared->lastIndex = newLastIndex;
            for (size_t i = firstEntryToProcess; i <= shared->lastIndex; ++i) {
                const auto& entry = shared->logKeeper->operator[](i);
                if (entry.command == nullptr) {
                    continue;
                }
                const auto commandType = entry.command->GetType();
                if (commandType == "SingleConfiguration") {
                    const auto command = std::static_pointer_cast< Raft::SingleConfigurationCommand >(entry.command);
                    shared->clusterConfiguration = command->configuration;
                    OnSetClusterConfiguration();
                }
            }
        }

        /**
         * Update the commit index of the log, processing all log entries
         * between the previous commit index and the new commit index.
         *
         * @param[in] newCommitIndex
         *     This is the new value to set for the commit index.
         */
        void AdvanceCommitIndex(size_t newCommitIndex) {
            const auto lastCommitIndex = shared->commitIndex;
            shared->commitIndex = newCommitIndex;
            shared->logKeeper->Commit(shared->commitIndex);
            for (size_t i = lastCommitIndex + 1; i <= shared->commitIndex; ++i) {
                const auto& entry = shared->logKeeper->operator[](i);
                if (entry.command == nullptr) {
                    continue;
                }
                const auto commandType = entry.command->GetType();
                if (commandType == "SingleConfiguration") {
                    const auto command = std::static_pointer_cast< Raft::SingleConfigurationCommand >(entry.command);
                    if (
                        (shared->electionState == ElectionState::Leader)
                        && (
                            shared->clusterConfiguration.instanceIds.find(
                                shared->serverConfiguration.selfInstanceId
                            ) == shared->clusterConfiguration.instanceIds.end()
                        )
                    ) {
                        RevertToFollower();
                    }
                }
            }
        }

        /**
         * Perform any work that is required when the cluster configuration
         * changes.
         */
        void OnSetClusterConfiguration() {
            if (
                shared->clusterConfiguration.instanceIds.find(
                    shared->serverConfiguration.selfInstanceId
                ) == shared->clusterConfiguration.instanceIds.end()
            ) {
                shared->isVotingMember = false;
            } else {
                shared->isVotingMember = true;
            }
        }

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
        ) {
            const auto now = timeKeeper->GetCurrentTime();
            Message response;
            response.type = Message::Type::RequestVoteResults;
            response.requestVoteResults.term = std::max(
                shared->persistentStateCache.currentTerm,
                messageDetails.term
            );
            const auto lastIndex = shared->lastIndex;
            const auto lastTerm = (
                (shared->lastIndex == 0)
                ? 0
                : shared->logKeeper->operator[](shared->lastIndex).term
            );
            if (shared->persistentStateCache.currentTerm > messageDetails.term) {
                shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                    1,
                    "Rejecting vote for server %u (old term %u < %u)",
                    senderInstanceNumber,
                    messageDetails.term,
                    shared->persistentStateCache.currentTerm
                );
                response.requestVoteResults.voteGranted = false;
            } else if (
                (shared->persistentStateCache.currentTerm == messageDetails.term)
                && shared->persistentStateCache.votedThisTerm
                && (shared->persistentStateCache.votedFor != senderInstanceNumber)
            ) {
                shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                    1,
                    "Rejecting vote for server %u (already voted for %u for term %u -- we are in term %u)",
                    senderInstanceNumber,
                    shared->persistentStateCache.votedFor,
                    messageDetails.term,
                    shared->persistentStateCache.currentTerm
                );
                response.requestVoteResults.voteGranted = false;
            } else if (
                (lastTerm > messageDetails.lastLogTerm)
                || (
                    (lastTerm == messageDetails.lastLogTerm)
                    && (lastIndex > messageDetails.lastLogIndex)
                )
            ) {
                shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                    1,
                    "Rejecting vote for server %u (our log at %d:%d is more up to date than theirs at %d:%d)",
                    senderInstanceNumber,
                    lastIndex,
                    lastTerm,
                    messageDetails.lastLogIndex,
                    messageDetails.lastLogTerm
                );
                response.requestVoteResults.voteGranted = false;
            } else {
                shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                    1,
                    "Voting for server %u for term %u (we were in term %u)",
                    senderInstanceNumber,
                    messageDetails.term,
                    shared->persistentStateCache.currentTerm
                );
                response.requestVoteResults.voteGranted = true;
                shared->persistentStateCache.votedThisTerm = true;
                shared->persistentStateCache.votedFor = senderInstanceNumber;
                shared->persistentStateKeeper->Save(shared->persistentStateCache);
                UpdateCurrentTerm(messageDetails.term);
                RevertToFollower();
            }
            QueueMessageToBeSent(response, senderInstanceNumber, now);
        }

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
        ) {
            auto& instance = shared->instances[senderInstanceNumber];
            if (messageDetails.term < shared->persistentStateCache.currentTerm) {
                shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                    1,
                    "Stale vote from server %u in term %u ignored",
                    senderInstanceNumber,
                    messageDetails.term
                );
                return;
            }
            if (messageDetails.voteGranted) {
                if (instance.awaitingResponse) {
                    ++shared->votesForUs;
                    shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                        1,
                        "Server %u voted for us in term %u (%zu/%zu)",
                        senderInstanceNumber,
                        shared->persistentStateCache.currentTerm,
                        shared->votesForUs,
                        shared->clusterConfiguration.instanceIds.size()
                    );
                    if (
                        (shared->electionState == IServer::ElectionState::Candidate)
                        && (
                            shared->votesForUs
                            > shared->clusterConfiguration.instanceIds.size() - shared->votesForUs
                        )
                    ) {
                        AssumeLeadership();
                    }
                } else {
                    shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                        1,
                        "Repeat vote from server %u in term %u ignored",
                        senderInstanceNumber,
                        messageDetails.term
                    );
                }
            } else {
                shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                    1,
                    "Server %u refused to voted for us in term %u",
                    senderInstanceNumber,
                    shared->persistentStateCache.currentTerm
                );
                if (messageDetails.term > shared->persistentStateCache.currentTerm) {
                    UpdateCurrentTerm(messageDetails.term);
                    RevertToFollower();
                }
            }
            instance.awaitingResponse = false;
        }

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
        ) {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Received AppendEntries(%zu entries building on %zu from term %d) from server %d in term %d (we are in term %d)",
                entries.size(),
                messageDetails.prevLogIndex,
                messageDetails.prevLogTerm,
                senderInstanceNumber,
                messageDetails.term,
                shared->persistentStateCache.currentTerm
            );
            if (shared->persistentStateCache.currentTerm > messageDetails.term) {
                return;
            }
            if (
                (shared->electionState != ElectionState::Leader)
                || (shared->persistentStateCache.currentTerm < messageDetails.term)
            ) {
                UpdateCurrentTerm(messageDetails.term);
                if (!shared->thisTermLeaderAnnounced) {
                    shared->thisTermLeaderAnnounced = true;
                    QueueLeadershipChangeAnnouncement(
                        senderInstanceNumber,
                        shared->persistentStateCache.currentTerm
                    );
                }
            }
            RevertToFollower();
            AdvanceCommitIndex(messageDetails.leaderCommit);
            Message response;
            response.type = Message::Type::AppendEntriesResults;
            response.appendEntriesResults.term = shared->persistentStateCache.currentTerm;
            if (
                (messageDetails.prevLogIndex > shared->lastIndex)
                || (
                    shared->logKeeper->operator[](messageDetails.prevLogIndex).term
                    != messageDetails.prevLogTerm
                )
            ) {
                response.appendEntriesResults.success = false;
                response.appendEntriesResults.matchIndex = 0;
            } else {
                response.appendEntriesResults.success = true;
                size_t nextIndex = messageDetails.prevLogIndex + 1;
                std::vector< LogEntry > entriesToAdd;
                bool conflictFound = false;
                for (size_t i = 0; i < entries.size(); ++i) {
                    auto& newEntry = entries[i];
                    const auto logIndex = messageDetails.prevLogIndex + i + 1;
                    if (
                        conflictFound
                        || (logIndex > shared->lastIndex)
                    ) {
                        entriesToAdd.push_back(std::move(newEntry));
                    } else {
                        const auto& oldEntry = shared->logKeeper->operator[](logIndex);
                        if (oldEntry.term != newEntry.term) {
                            conflictFound = true;
                            shared->logKeeper->RollBack(logIndex - 1);
                            entriesToAdd.push_back(std::move(newEntry));
                        }
                    }
                }
                if (!entriesToAdd.empty()) {
                    shared->logKeeper->Append(entriesToAdd);
                }
                SetLastIndex(shared->logKeeper->GetSize());
                response.appendEntriesResults.matchIndex = shared->lastIndex;
            }
            const auto now = timeKeeper->GetCurrentTime();
            QueueMessageToBeSent(response, senderInstanceNumber, now);
        }

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
        ) {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Received AppendEntriesResults(%s, term %d) from server %d (we are %s in term %d)",
                (messageDetails.success ? "success" : "failure"),
                messageDetails.term,
                senderInstanceNumber,
                ElectionStateToString(shared->electionState).c_str(),
                shared->persistentStateCache.currentTerm
            );
            if (shared->electionState != ElectionState::Leader) {
                return;
            }
            auto& instance = shared->instances[senderInstanceNumber];
            instance.awaitingResponse = false;
            if (messageDetails.success) {
                instance.matchIndex = messageDetails.matchIndex;
                instance.nextIndex = messageDetails.matchIndex + 1;
                if (instance.nextIndex <= shared->lastIndex) {
                    AttemptLogReplication(senderInstanceNumber);
                }
            } else {
                // TODO: potential optimization: set nextIndex = matchIndex
                // from the results.
                --instance.nextIndex;
                AttemptLogReplication(senderInstanceNumber);
            }
            std::map< size_t, size_t > indexMatchCounts;
            for (const auto& instance: shared->instances) {
                if (instance.first == shared->serverConfiguration.selfInstanceId) {
                    continue;
                }
                ++indexMatchCounts[instance.second.matchIndex];
            }
            size_t totalMatchCounts = 0;
            for (
                auto indexMatchCountEntry = indexMatchCounts.rbegin();
                indexMatchCountEntry != indexMatchCounts.rend();
                ++indexMatchCountEntry
            ) {
                totalMatchCounts += indexMatchCountEntry->second;
                if (
                    (indexMatchCountEntry->first > shared->commitIndex)
                    && (totalMatchCounts + 1 > shared->instances.size() - totalMatchCounts - 1)
                    && (shared->logKeeper->operator[](indexMatchCountEntry->first).term == shared->persistentStateCache.currentTerm)
                ) {
                    AdvanceCommitIndex(indexMatchCountEntry->first);
                    break;
                }
            }
        }

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
        ) {
            const auto rpcTimeoutMilliseconds = (int)(
                shared->serverConfiguration.rpcTimeout * 1000.0
            );
            (void)workerAskedToStopOrWakeUp.wait_for(
                lock,
                std::chrono::milliseconds(rpcTimeoutMilliseconds),
                [this, &workerAskedToStop]{
                    return (
                        (
                            workerAskedToStop.wait_for(std::chrono::seconds(0))
                            == std::future_status::ready
                        )
                        || (shared->workerLoopCompletion != nullptr)
                        || !shared->messagesToBeSent.empty()
                    );
                }
            );
        }

        /**
         * This is the logic to perform once per worker thread loop.
         *
         * @param[in,out] lock
         *     This is the object used to manage the shared properties mutex in
         *     the worker thread.
         */
        void WorkerLoopBody(
            std::unique_lock< decltype(shared->mutex) >& lock
        ) {
            const auto now = timeKeeper->GetCurrentTime();
            const auto timeSinceLastLeaderMessage = (
                now - shared->timeOfLastLeaderMessage
            );
            if (shared->electionState == IServer::ElectionState::Leader) {
                if (
                    timeSinceLastLeaderMessage
                    >= shared->serverConfiguration.minimumElectionTimeout / 2
                ) {
                    QueueHeartBeatsToBeSent(now);
                }
            } else {
                if (
                    timeSinceLastLeaderMessage
                    >= shared->currentElectionTimeout
                ) {
                    StartElection(now);
                }
            }
            QueueRetransmissionsToBeSent(now);
            SendQueuedMessages(lock);
            SendQueuedLeadershipAnnouncements(lock);
        }

        /**
         * This runs in a thread and performs any background tasks required of
         * the Server, such as starting an election if no message is received
         * from the cluster leader before the next timeout.
         */
        void Worker() {
            std::unique_lock< decltype(shared->mutex) > lock(shared->mutex);
            shared->diagnosticsSender.SendDiagnosticInformationString(
                0,
                "Worker thread started"
            );
            ResetElectionTimer();
            auto workerAskedToStop = stopWorker.get_future();
            while (
                workerAskedToStop.wait_for(std::chrono::seconds(0))
                != std::future_status::ready
            ) {
                WaitForWork(lock, workerAskedToStop);
                const auto signalWorkerLoopCompleted = (
                    shared->workerLoopCompletion != nullptr
                );
                WorkerLoopBody(lock);
                if (signalWorkerLoopCompleted) {
                    shared->workerLoopCompletion->set_value();
                    shared->workerLoopCompletion = nullptr;
                }
            }
            if (shared->workerLoopCompletion != nullptr) {
                shared->workerLoopCompletion->set_value();
                shared->workerLoopCompletion = nullptr;
            }
            shared->diagnosticsSender.SendDiagnosticInformationString(
                0,
                "Worker thread stopping"
            );
        }
    };

    Server::~Server() noexcept = default;
    Server::Server(Server&&) noexcept = default;
    Server& Server::operator=(Server&&) noexcept = default;

    Server::Server()
        : impl_(new Impl())
    {
        SystemAbstractions::CryptoRandom jim;
        int seed;
        jim.Generate(&seed, sizeof(seed));
        impl_->shared->rng.seed(seed);
    }

    SystemAbstractions::DiagnosticsSender::UnsubscribeDelegate Server::SubscribeToDiagnostics(
        SystemAbstractions::DiagnosticsSender::DiagnosticMessageDelegate delegate,
        size_t minLevel
    ) {
        return impl_->shared->diagnosticsSender.SubscribeToDiagnostics(delegate, minLevel);
    }

    void Server::SetTimeKeeper(std::shared_ptr< TimeKeeper > timeKeeper) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->timeKeeper = timeKeeper;
    }

    void Server::WaitForAtLeastOneWorkerLoop() {
        std::unique_lock< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->shared->workerLoopCompletion = std::make_shared< std::promise< void > >();
        auto workerLoopWasCompleted = impl_->shared->workerLoopCompletion->get_future();
        impl_->workerAskedToStopOrWakeUp.notify_one();
        lock.unlock();
        workerLoopWasCompleted.wait();
    }

    size_t Server::GetCommitIndex() const {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        return impl_->shared->commitIndex;
    }

    void Server::SetCommitIndex(size_t commitIndex) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->shared->commitIndex = commitIndex;
    }

    size_t Server::GetLastIndex() const {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        return impl_->shared->lastIndex;
    }

    void Server::SetLastIndex(size_t lastIndex) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->shared->lastIndex = lastIndex;
    }

    size_t Server::GetNextIndex(int instanceId) const {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        return impl_->shared->instances[instanceId].nextIndex;
    }

    size_t Server::GetMatchIndex(int instanceId) const {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        return impl_->shared->instances[instanceId].matchIndex;
    }

    bool Server::IsVotingMember() const {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        return impl_->shared->isVotingMember;
    }

    void Server::SetSendMessageDelegate(SendMessageDelegate sendMessageDelegate) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->sendMessageDelegate = sendMessageDelegate;
    }

    void Server::SetLeadershipChangeDelegate(LeadershipChangeDelegate leadershipChangeDelegate) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->leadershipChangeDelegate = leadershipChangeDelegate;
    }

    void Server::Mobilize(
        std::shared_ptr< ILog > logKeeper,
        std::shared_ptr< IPersistentState > persistentStateKeeper,
        const ClusterConfiguration& clusterConfiguration,
        const ServerConfiguration& serverConfiguration
    ) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        if (impl_->worker.joinable()) {
            return;
        }
        impl_->shared->logKeeper = logKeeper;
        impl_->shared->persistentStateKeeper = persistentStateKeeper;
        impl_->shared->clusterConfiguration = clusterConfiguration;
        impl_->shared->serverConfiguration = serverConfiguration;
        impl_->shared->persistentStateCache = persistentStateKeeper->Load();
        impl_->shared->instances.clear();
        impl_->shared->electionState = IServer::ElectionState::Follower;
        impl_->shared->timeOfLastLeaderMessage = 0.0;
        impl_->shared->votesForUs = 0;
        impl_->SetLastIndex(logKeeper->GetSize());
        impl_->stopWorker = std::promise< void >();
        impl_->worker = std::thread(&Impl::Worker, impl_.get());
    }

    void Server::Demobilize() {
        std::unique_lock< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        if (!impl_->worker.joinable()) {
            return;
        }
        impl_->stopWorker.set_value();
        impl_->workerAskedToStopOrWakeUp.notify_one();
        lock.unlock();
        impl_->worker.join();
        impl_->shared->persistentStateKeeper = nullptr;
        impl_->shared->logKeeper = nullptr;
    }

    void Server::ReceiveMessage(
        const std::string& serializedMessage,
        int senderInstanceNumber
    ) {
        Message message(serializedMessage);
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        switch (message.type) {
            case Message::Type::RequestVote: {
                impl_->OnReceiveRequestVote(message.requestVote, senderInstanceNumber);
            } break;

            case Message::Type::RequestVoteResults: {
                impl_->OnReceiveRequestVoteResults(message.requestVoteResults, senderInstanceNumber);
            } break;

            case Message::Type::AppendEntries: {
                impl_->OnReceiveAppendEntries(
                    message.appendEntries,
                    std::move(message.log),
                    senderInstanceNumber
                );
            } break;

            case Message::Type::AppendEntriesResults: {
                impl_->OnReceiveAppendEntriesResults(
                    message.appendEntriesResults,
                    senderInstanceNumber
                );
            } break;

            default: {
            } break;
        }
    }

    auto Server::GetElectionState() -> ElectionState {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        return impl_->shared->electionState;
    }

    void Server::AppendLogEntries(const std::vector< LogEntry >& entries) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        if (impl_->shared->electionState != ElectionState::Leader) {
            return;
        }
        impl_->shared->logKeeper->Append(entries);
        const auto now = impl_->timeKeeper->GetCurrentTime();
        impl_->SetLastIndex(impl_->shared->lastIndex + entries.size());
        impl_->QueueAppendEntriesToBeSent(now, entries);
    }

}
