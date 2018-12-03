/**
 * @file Server.cpp
 *
 * This module contains the implementation of the Raft::Server class.
 *
 * © 2018 by Richard Walters
 */

#include "MessageImpl.hpp"

#include <future>
#include <map>
#include <mutex>
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
         * This holds all configuration items for the server.
         */
        Raft::IServer::Configuration configuration;

        /**
         * This is used to synchronize access to the properties below.
         */
        std::mutex mutex;

        /**
         * This is a standard C++ Mersenne Twister pseudo-random number
         * generator, used to pick election timeouts.
         */
        std::mt19937 rng;

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
         * This holds information this server tracks about the other servers.
         */
        std::map< unsigned int, InstanceInfo > instances;

        // Methods

        ServerSharedProperties()
            : diagnosticsSender("Raft::Server")
        {
        }
    };

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
         * This thread performs any background tasks required of the
         * server, such as starting an election if no message is received
         * from the cluster leader before the next timeout.
         */
        std::thread worker;

        /**
         * This is set when the worker thread should stop.
         */
        std::promise< void > stopWorker;

        // Methods

        /**
         * This method is called whenever a message is received from the
         * cluster leader, or when the server starts an election, or starts up
         * initially.  It samples the current time from the time keeper and
         * stores it in the timeOfLastLeaderMessage shared property.  It also
         * picks a new election timeout.
         */
        void UpdateTimeOfLastLeaderMessage() {
            std::lock_guard< decltype(shared->mutex) > lock(shared->mutex);
            shared->timeOfLastLeaderMessage = timeKeeper->GetCurrentTime();
            shared->currentElectionTimeout = std::uniform_real_distribution<>(
                shared->configuration.minimumElectionTimeout,
                shared->configuration.maximumElectionTimeout
            )(shared->rng);
        }

        /**
         * This method returns the amount in time (in seconds) since the server
         * started or received the last message from a cluster leader.
         *
         * @param[in] now
         *     This is the current time according to the time keeper.
         *
         * @return
         *     The amount in time (in seconds) since the server started or
         *     received the last message from a cluster leader is returned.
         */
        double GetTimeSinceLastLeaderMessage(double now) {
            std::lock_guard< decltype(shared->mutex) > lock(shared->mutex);
            return now - shared->timeOfLastLeaderMessage;
        }

        /**
         * This method checks to see if another thread has requested that we
         * set a promised value once the worker thread has executed one full
         * loop.
         *
         * @return
         *     An indication of whether or not another thread has requested
         *     that we set a promised value once the worker thread has executed
         *     one full loop is returned.
         */
        bool MakeWorkerThreadLoopPromiseIfNeeded() {
            std::lock_guard< decltype(shared->mutex) > lock(shared->mutex);
            return (shared->workerLoopCompletion != nullptr);
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
        void SendMessage(
            std::shared_ptr< Message > message,
            unsigned int instanceNumber,
            double now
        ) {
            auto& instance = shared->instances[instanceNumber];
            instance.timeLastRequestSent = now;
            instance.lastRequest = message;
            sendMessageDelegate(message, instanceNumber);
        }

        /**
         * This method starts a new election for leader of the server cluster.
         *
         * @param[in] now
         *     This is the current time according to the time keeper.
         */
        void StartElection(double now) {
            std::lock_guard< decltype(shared->mutex) > lock(shared->mutex);
            shared->votesForUs = 1;
            const auto message = Message::CreateMessage();
            message->impl_->type = MessageImpl::Type::RequestVote;
            message->impl_->requestVote.candidateId = shared->configuration.selfInstanceNumber;
            message->impl_->requestVote.term = ++shared->configuration.currentTerm;
            shared->diagnosticsSender.SendDiagnosticInformationString(
                1,
                "Timeout -- starting new election"
            );
            for (auto& instanceEntry: shared->instances) {
                instanceEntry.second.awaitingVote = false;
            }
            for (auto instanceNumber: shared->configuration.instanceNumbers) {
                if (instanceNumber == shared->configuration.selfInstanceNumber) {
                    continue;
                }
                auto& instance = shared->instances[instanceNumber];
                instance.awaitingVote = true;
                SendMessage(message, instanceNumber, now);
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
        void DoRetransmissions(double now) {
            std::lock_guard< decltype(shared->mutex) > lock(shared->mutex);
            for (auto& instanceEntry: shared->instances) {
                if (
                    instanceEntry.second.awaitingVote
                    && (now - instanceEntry.second.timeLastRequestSent >= shared->configuration.rpcTimeout)
                ) {
                    SendMessage(
                        instanceEntry.second.lastRequest,
                        instanceEntry.first,
                        now
                    );
                }
            }
        }

        /**
         * This runs in a thread and performs any background tasks required of
         * the Server, such as starting an election if no message is received
         * from the cluster leader before the next timeout.
         */
        void Worker() {
            shared->diagnosticsSender.SendDiagnosticInformationString(
                0,
                "Worker thread started"
            );
            UpdateTimeOfLastLeaderMessage();
            auto workerAskedToStop = stopWorker.get_future();
            const auto rpcTimeoutMilliseconds = (int)(shared->configuration.rpcTimeout * 1000.0);
            while (
                workerAskedToStop.wait_for(
                    std::chrono::milliseconds(rpcTimeoutMilliseconds)
                ) != std::future_status::ready
            ) {
                const auto signalWorkerLoopCompleted = MakeWorkerThreadLoopPromiseIfNeeded();
                const auto now = timeKeeper->GetCurrentTime();
                const auto timeSinceLastLeaderMessage = GetTimeSinceLastLeaderMessage(now);
                if (timeSinceLastLeaderMessage >= shared->currentElectionTimeout) {
                    StartElection(now);
                }
                DoRetransmissions(now);
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
        impl_->timeKeeper = timeKeeper;
    }

    auto Server::GetConfiguration() const -> const Configuration& {
        return impl_->shared->configuration;
    }

    void Server::Mobilize() {
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
        if (!impl_->worker.joinable()) {
            return;
        }
        impl_->stopWorker.set_value();
        impl_->worker.join();
    }

    void Server::WaitForAtLeastOneWorkerLoop() {
        std::unique_lock< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->shared->workerLoopCompletion = std::make_shared< std::promise< void > >();
        auto workerLoopWasCompleted = impl_->shared->workerLoopCompletion->get_future();
        lock.unlock();
        workerLoopWasCompleted.wait();
    }

    bool Server::Configure(const Configuration& configuration) {
        impl_->shared->configuration = configuration;
        return true;
    }

    void Server::SetSendMessageDelegate(SendMessageDelegate sendMessageDelegate) {
        impl_->sendMessageDelegate = sendMessageDelegate;
    }

    void Server::ReceiveMessage(
        std::shared_ptr< Message > message,
        unsigned int senderInstanceNumber
    ) {
        switch (message->impl_->type) {
            case MessageImpl::Type::RequestVoteResults: {
                auto& instance = impl_->shared->instances[senderInstanceNumber];
                instance.awaitingVote = false;
                ++impl_->shared->votesForUs;
                if (impl_->shared->votesForUs >= impl_->shared->configuration.instanceNumbers.size() / 2 + 1) {
                    impl_->shared->isLeader = true;
                }
            } break;

            default: {
            } break;
        }
    }

    bool Server::IsLeader() {
        return impl_->shared->isLeader;
    }

}
