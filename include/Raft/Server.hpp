#ifndef RAFT_SERVER_HPP
#define RAFT_SERVER_HPP

/**
 * @file Server.hpp
 *
 * This module declares the Raft::Server implementation.
 *
 * © 2018 by Richard Walters
 */

#include <memory>
#include <Raft/IServer.hpp>
#include <Raft/TimeKeeper.hpp>
#include <stddef.h>
#include <SystemAbstractions/DiagnosticsSender.hpp>

namespace Raft {

    /**
     * This class represents one member of the server cluster.
     */
    class Server
        : public IServer
    {
        // Lifecycle Methods
    public:
        ~Server() noexcept;
        Server(const Server&) = delete;
        Server(Server&&) noexcept;
        Server& operator=(const Server&) = delete;
        Server& operator=(Server&&) noexcept;

        // Public Methods
    public:
        /**
         * This is the constructor of the class.
         */
        Server();

        /**
         * This method forms a new subscription to diagnostic
         * messages published by the class.
         *
         * @param[in] delegate
         *     This is the function to call to deliver messages
         *     to the subscriber.
         *
         * @param[in] minLevel
         *     This is the minimum level of message that this subscriber
         *     desires to receive.
         *
         * @return
         *     A function is returned which may be called
         *     to terminate the subscription.
         */
        SystemAbstractions::DiagnosticsSender::UnsubscribeDelegate SubscribeToDiagnostics(
            SystemAbstractions::DiagnosticsSender::DiagnosticMessageDelegate delegate,
            size_t minLevel = 0
        );

        /**
         * This method is called to provide the object used to track time
         * for the server.
         *
         * @param[in] timeKeeper
         *     This is the object used to track time for the server.
         */
        void SetTimeKeeper(std::shared_ptr< TimeKeeper > timeKeeper);

        /**
         * This method returns the server's current configuration.
         *
         * @return
         *     The server's current configuration is returned.
         */
        const Configuration& GetConfiguration() const;

        /**
         * This method blocks until the Coordinator's worker thread executes at
         * least one more loop.
         */
        void WaitForAtLeastOneWorkerLoop();

        /**
         * Return the current commit index of the server.  The commit index is
         * the index of the last log entry that is known to have been appended
         * to the logs of a majority of servers in the cluster.  Only committed
         * log entries may be applied to server state machines.  Otherwise,
         * servers may not correctly replicate state, especially when servers
         * fail and new servers take over leadership.
         *
         * @return
         *     The current commit index of the server is returned.
         */
        size_t GetCommitIndex() const;

        /**
         * Record that the cluster has committed the log up to the given index.
         *
         * @note
         *     This is only to be used by test frameworks.
         *
         * @param[in] commitIndex
         *     This is the index of the last log entry that is known to have
         *     been appended to the logs of a majority of servers in the
         *     cluster.
         */
        void SetCommitIndex(size_t commitIndex);

        /**
         * This method puts the server back into the state it was in when
         * first mobilized.
         */
        void Reset();

        // IServer
    public:
        virtual bool Configure(const Configuration& configuration) override;
        virtual void SetCreateMessageDelegate(CreateMessageDelegate createMessageDelegate) override;
        virtual void SetSendMessageDelegate(SendMessageDelegate sendMessageDelegate) override;
        virtual void SetLeadershipChangeDelegate(LeadershipChangeDelegate leadershipChangeDelegate) override;
        virtual void SetAppendEntriesDelegate(AppendEntriesDelegate appendEntriesDelegate) override;
        virtual void Mobilize() override;
        virtual void Demobilize() override;
        virtual void ReceiveMessage(
            std::shared_ptr< Message > message,
            int senderInstanceNumber
        ) override;
        virtual ElectionState GetElectionState() override;
        virtual void AppendLogEntries(const std::vector< LogEntry >& entries) override;

        // Private properties
    private:
        /**
         * This is the type of structure that contains the private
         * properties of the instance.  It is defined in the implementation
         * and declared here to ensure that it is scoped inside the class.
         */
        struct Impl;

        /**
         * This contains the private properties of the instance.
         */
        std::unique_ptr< Impl > impl_;
    };

}

#endif /* RAFT_SERVER_HPP */
