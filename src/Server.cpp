/**
 * @file Server.cpp
 *
 * This module contains the implementation of the Raft::Server class.
 *
 * © 2018 by Richard Walters
 */

#include "MessageImpl.hpp"

#include <algorithm>
#include <future>
#include <map>
#include <mutex>
#include <queue>
#include <Raft/Server.hpp>
#include <Raft/TimeKeeper.hpp>
#include <random>
#include <SystemAbstractions/DiagnosticsSender.hpp>
#include <thread>
#include <time.h>

namespace {

    /**
     * This holds information that one server holds about another server.
     */
    struct InstanceInfo {
        /**
         * During an election, this indicates whether or not we're still
         * awaiting a RequestVote response from this instance.
         */
        bool awaitingVote = false;

        /**
         * This is the time, according to the time keeper, that a request was
         * last sent to the instance.
         */
        double timeLastRequestSent = 0.0;

        /**
         * This is the last request sent to the instance.
         */
        std::shared_ptr< Raft::Message > lastRequest;
    };

    /**
     * This holds information used to store a message to be sent, and later to
     * send the message.
     */
    struct MessageToBeSent {
        /**
         * This is the message to be sent.
         */
        std::shared_ptr< Raft::Message > message;

        /**
         * This is the unique identifier of the server to which to send the
         * message.
         */
        unsigned int receiverInstanceNumber = 0;
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
        unsigned int leaderId = 0;

        /**
         * This is the generation number of the server cluster leadership,
         * which is incremented whenever a new election is started.
         */
        unsigned int term = 0;
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
         * This holds all configuration items for the server.
         */
        Raft::IServer::Configuration configuration;

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
         * This indicates whether or not the server is currently the leader of
         * the cluster.
         */
        bool isLeader = false;

        /**
         * During an election, this is the number of votes we have received
         * for ourselves.
         */
        size_t votesForUs = 0;

        /**
         * This indicates whether or not the server has voted for another
         * server to be the leader this term.
         */
        bool votedThisTerm = false;

        /**
         * If the server has voted for another server to be the leader this
         * term, this is the unique identifier of the server for whom we voted.
         */
        unsigned int votedFor = 0;

        /**
         * This holds information this server tracks about the other servers.
         */
        std::map< unsigned int, InstanceInfo > instances;

        // Methods

        ServerSharedProperties()
            : diagnosticsSender("Raft::Server")
        {
        }
    };

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
         * This is the delegate to be called later whenever the server
         * wants to create a message object.
         */
        CreateMessageDelegate createMessageDelegate;

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
                shared->configuration.minimumElectionTimeout,
                shared->configuration.maximumElectionTimeout
            )(shared->rng);
        }

        /**
         * This method sends the given message to the instance with the given
         * unique identifier.
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
            std::shared_ptr< Message > message,
            unsigned int instanceNumber,
            double now
        ) {
            auto& instance = shared->instances[instanceNumber];
            instance.timeLastRequestSent = now;
            instance.lastRequest = message;
            MessageToBeSent messageToBeSent;
            messageToBeSent.message = message;
            messageToBeSent.receiverInstanceNumber = instanceNumber;
            shared->messagesToBeSent.push(std::move(messageToBeSent));
            workerAskedToStopOrWakeUp.notify_one();
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
            unsigned int leaderId,
            unsigned int term
        ) {
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
            shared->votedThisTerm = true;
            shared->votedFor = shared->configuration.selfInstanceNumber;
            shared->votesForUs = 1;
            for (auto& instanceEntry: shared->instances) {
                instanceEntry.second.awaitingVote = false;
            }
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Timeout -- starting new election (term %u)",
                shared->configuration.currentTerm
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
            const auto message = createMessageDelegate();
            message->impl_->type = MessageImpl::Type::RequestVote;
            message->impl_->requestVote.candidateId = shared->configuration.selfInstanceNumber;
            message->impl_->requestVote.term = shared->configuration.currentTerm;
            for (auto instanceNumber: shared->configuration.instanceNumbers) {
                if (instanceNumber == shared->configuration.selfInstanceNumber) {
                    continue;
                }
                auto& instance = shared->instances[instanceNumber];
                instance.awaitingVote = true;
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
            ++shared->configuration.currentTerm;
            StepUpAsCandidate();
            SendInitialVoteRequests(now);
        }

        /**
         * This method sends a heartbeat message to all other servers in the
         * server cluster.
         *
         * @param[in] now
         *     This is the current time according to the time keeper.
         */
        void QueueHeartBeatsToBeSent(double now) {
            const auto message = createMessageDelegate();
            message->impl_->type = MessageImpl::Type::HeartBeat;
            message->impl_->heartbeat.term = shared->configuration.currentTerm;
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                0,
                "Sending heartbeat (term %u)",
                shared->configuration.currentTerm
            );
            for (auto instanceNumber: shared->configuration.instanceNumbers) {
                if (instanceNumber == shared->configuration.selfInstanceNumber) {
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
                    instanceEntry.second.awaitingVote
                    && (now - instanceEntry.second.timeLastRequestSent >= shared->configuration.rpcTimeout)
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
            if (leadershipChangeDelegate == nullptr) {
                return;
            }
            decltype(shared->leadershipAnnouncementsToBeSent) leadershipAnnouncementsToBeSent;
            leadershipAnnouncementsToBeSent.swap(shared->leadershipAnnouncementsToBeSent);
            auto leadershipChangeDelegateCopy = leadershipChangeDelegate;
            lock.unlock();
            SendLeadershipAnnouncements(
                leadershipChangeDelegateCopy,
                std::move(leadershipAnnouncementsToBeSent)
            );
            lock.lock();
        }

        /**
         * This method updates the server state to make the server a
         * "follower", as in not seeking election, and not the leader.
         */
        void RevertToFollower() {
            for (auto& instanceEntry: shared->instances) {
                instanceEntry.second.awaitingVote = false;
            }
            shared->isLeader = false;
            ResetElectionTimer();
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
            const MessageImpl::RequestVoteDetails& messageDetails,
            unsigned int senderInstanceNumber
        ) {
            const auto now = timeKeeper->GetCurrentTime();
            const auto response = createMessageDelegate();
            response->impl_->type = MessageImpl::Type::RequestVoteResults;
            response->impl_->requestVoteResults.term = std::max(
                shared->configuration.currentTerm,
                messageDetails.term
            );
            if (shared->configuration.currentTerm > messageDetails.term) {
                shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                    1,
                    "Rejecting vote for server %u (old term %u < %u)",
                    senderInstanceNumber,
                    messageDetails.term,
                    shared->configuration.currentTerm
                );
                response->impl_->requestVoteResults.voteGranted = false;
            } else if (
                (shared->configuration.currentTerm == messageDetails.term)
                && shared->votedThisTerm
                && (shared->votedFor != senderInstanceNumber)
            ) {
                shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                    1,
                    "Rejecting vote for server %u (already voted for %u for term %u -- we are in term %u)",
                    senderInstanceNumber,
                    shared->votedFor,
                    messageDetails.term,
                    shared->configuration.currentTerm
                );
                response->impl_->requestVoteResults.voteGranted = false;
            } else {
                shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                    1,
                    "Voting for server %u for term %u (we were in term %u)",
                    senderInstanceNumber,
                    messageDetails.term,
                    shared->configuration.currentTerm
                );
                response->impl_->requestVoteResults.voteGranted = true;
                shared->votedThisTerm = true;
                shared->votedFor = senderInstanceNumber;
                shared->configuration.currentTerm = messageDetails.term;
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
            const MessageImpl::RequestVoteResultsDetails& messageDetails,
            unsigned int senderInstanceNumber
        ) {
            auto& instance = shared->instances[senderInstanceNumber];
            if (messageDetails.voteGranted) {
                if (instance.awaitingVote) {
                    ++shared->votesForUs;
                    shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                        1,
                        "Server %u voted for us in term %u (%zu/%zu)",
                        senderInstanceNumber,
                        shared->configuration.currentTerm,
                        shared->votesForUs,
                        shared->configuration.instanceNumbers.size()
                    );
                    if (shared->votesForUs >= shared->configuration.instanceNumbers.size() / 2 + 1) {
                        shared->isLeader = true;
                        shared->diagnosticsSender.SendDiagnosticInformationString(
                            2,
                            "Received majority vote -- assuming leadership"
                        );
                    }
                    QueueLeadershipChangeAnnouncement(
                        shared->configuration.selfInstanceNumber,
                        shared->configuration.currentTerm
                    );
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
                    shared->configuration.currentTerm
                );
            }
            instance.awaitingVote = false;
        }

        /**
         * This method is called whenever the server receives a heartbeat
         * message from the cluster leader.
         *
         * @param[in] message
         *     This contains the details of the message received.
         *
         * @param[in] senderInstanceNumber
         *     This is the unique identifier of the server that sent the
         *     message.
         */
        void OnReceiveHeartBeat(
            const MessageImpl::HeartbeatDetails& messageDetails,
            unsigned int senderInstanceNumber
        ) {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Received heartbeat from server %u in term %u (we are in term %u)",
                senderInstanceNumber,
                messageDetails.term,
                shared->configuration.currentTerm
            );
            if (shared->configuration.currentTerm < messageDetails.term) {
                shared->configuration.currentTerm = messageDetails.term;
                RevertToFollower();
            } else if (shared->configuration.currentTerm == messageDetails.term) {
                ResetElectionTimer();
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
                shared->configuration.rpcTimeout * 1000.0
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
            if (shared->isLeader) {
                if (
                    timeSinceLastLeaderMessage
                    >= shared->configuration.minimumElectionTimeout / 2
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
        impl_->shared->rng.seed((int)time(NULL));
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

    auto Server::GetConfiguration() const -> const Configuration& {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        return impl_->shared->configuration;
    }

    void Server::WaitForAtLeastOneWorkerLoop() {
        std::unique_lock< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->shared->workerLoopCompletion = std::make_shared< std::promise< void > >();
        auto workerLoopWasCompleted = impl_->shared->workerLoopCompletion->get_future();
        impl_->workerAskedToStopOrWakeUp.notify_one();
        lock.unlock();
        workerLoopWasCompleted.wait();
    }

    bool Server::Configure(const Configuration& configuration) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->shared->configuration = configuration;
        return true;
    }

    void Server::SetCreateMessageDelegate(CreateMessageDelegate createMessageDelegate) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->createMessageDelegate = createMessageDelegate;
    }

    void Server::SetSendMessageDelegate(SendMessageDelegate sendMessageDelegate) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->sendMessageDelegate = sendMessageDelegate;
    }

    void Server::SetLeadershipChangeDelegate(LeadershipChangeDelegate leadershipChangeDelegate) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->leadershipChangeDelegate = leadershipChangeDelegate;
    }

    void Server::Mobilize() {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        if (impl_->worker.joinable()) {
            return;
        }
        impl_->shared->instances.clear();
        impl_->shared->isLeader = false;
        impl_->shared->timeOfLastLeaderMessage = 0.0;
        impl_->shared->votesForUs = 0;
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
    }

    void Server::ReceiveMessage(
        std::shared_ptr< Message > message,
        unsigned int senderInstanceNumber
    ) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        switch (message->impl_->type) {
            case MessageImpl::Type::RequestVote: {
                impl_->OnReceiveRequestVote(message->impl_->requestVote, senderInstanceNumber);
            } break;

            case MessageImpl::Type::RequestVoteResults: {
                impl_->OnReceiveRequestVoteResults(message->impl_->requestVoteResults, senderInstanceNumber);
            } break;

            case MessageImpl::Type::HeartBeat: {
                impl_->OnReceiveHeartBeat(message->impl_->heartbeat, senderInstanceNumber);
            } break;

            default: {
            } break;
        }
    }

    bool Server::IsLeader() {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        return impl_->shared->isLeader;
    }

}
