/**
 * @file Server.cpp
 *
 * This module contains the implementation of the Raft::Server class.
 *
 * © 2018 by Richard Walters
 */

#include "MessageImpl.hpp"

#include <future>
#include <mutex>
#include <Raft/Server.hpp>
#include <Raft/TimeKeeper.hpp>
#include <SystemAbstractions/DiagnosticsSender.hpp>
#include <thread>

namespace {

    /**
     * This is the amount of time to wait between polling for various
     * conditions in the worker thread of the server.
     */
    const std::chrono::milliseconds WORKER_POLLING_PERIOD = std::chrono::milliseconds(50);

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
         * If this is not nullptr, then the worker thread should set the result
         * once it executes a full loop.
         */
        std::shared_ptr< std::promise< void > > workerLoopCompletion;

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
         * This runs in a thread and performs any background tasks required of
         * the Server, such as starting an election if no message is received
         * from the cluster leader before the next timeout.
         */
        void Worker() {
            shared->diagnosticsSender.SendDiagnosticInformationString(
                0,
                "Worker thread started"
            );
            double timeOfLastLeaderMessage = timeKeeper->GetCurrentTime();
            auto workerAskedToStop = stopWorker.get_future();
            while (workerAskedToStop.wait_for(WORKER_POLLING_PERIOD) != std::future_status::ready) {
                const auto signalWorkerLoopCompleted = MakeWorkerThreadLoopPromiseIfNeeded();
                const auto now = timeKeeper->GetCurrentTime();
                const auto timeSinceLastLeaderMessage = now - timeOfLastLeaderMessage;
                if (timeSinceLastLeaderMessage >= shared->configuration.minimumTimeout) {
                    const auto message = Message::CreateMessage();
                    message->impl_->type = MessageImpl::Type::Election;
                    message->impl_->election.candidateId = shared->configuration.selfInstanceNumber;
                    shared->diagnosticsSender.SendDiagnosticInformationString(
                        1,
                        "Timeout -- starting new election"
                    );
                    sendMessageDelegate(message);
                }
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

    bool Server::Configure(const Configuration& configuration) {
        impl_->shared->configuration = configuration;
        return true;
    }

    void Server::SetSendMessageDelegate(SendMessageDelegate sendMessageDelegate) {
        impl_->sendMessageDelegate = sendMessageDelegate;
    }

    void Server::Mobilize() {
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

}
