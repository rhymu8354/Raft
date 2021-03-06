#pragma once

/**
 * @file Server.hpp
 *
 * This module declares the Raft::Server implementation.
 *
 * © 2018-2020 by Richard Walters
 */

#include "IServer.hpp"

#include <functional>
#include <memory>
#include <stddef.h>
#include <SystemAbstractions/DiagnosticsSender.hpp>
#include <Timekeeping/Scheduler.hpp>

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
         * Return the index of the last entry appended to the log.
         *
         * @return
         *     The index of the last entry appended to the log is returned.
         */
        size_t GetLastIndex() const;

        /**
         * Record that the server has appended entries to the log up to the
         * given index.
         *
         * @note
         *     This is only to be used by test frameworks.
         *
         * @param[in] lastIndex
         *     This is the index of the last log entry that is known to have
         *     been appended to the log of the server.
         */
        void SetLastIndex(size_t lastIndex);

        /**
         * Return the index of the next log entry to be sent to the server with
         * the given identifier.
         *
         * @param[in] instanceId
         *     This is the unique identifier of the server for which to return
         *     the next log entry index.
         *
         * @return
         *     The index of the next log entry to be sent to the server with
         *     the given identifier is returned.
         */
        size_t GetNextIndex(int instanceId) const;

        /**
         * Return the current match index for the server with the given
         * identifier.
         *
         * @param[in] instanceId
         *     This is the unique identifier of the server for which to return
         *     the current match index.
         *
         * @return
         *     The current match index for the server with the given
         *     identifier is returned.
         */
        size_t GetMatchIndex(int instanceId) const;

        /**
         * Return an indication of whether or not this server is a voting
         * member of the cluster.
         *
         * @return
         *     An indication of whether or not this server is a voting
         *     member of the cluster is returned.
         */
        bool IsVotingMember() const;

        /**
         * Return an indication of whether or not this server is currently
         * using a joint configuration set.
         *
         * @return
         *     An indication of whether or not this server is currently
         *     using a joint configuration set is returned.
         */
        bool HasJointConfiguration() const;

        /**
         * Return the unique identifier of the server leading the cluster.
         *
         * @return
         *     The unique identifier of the server leading the cluster
         *     is returned.
         *
         * @retval 0
         *     This is returned if the cluster leader is unknown.
         */
        int GetClusterLeaderId() const;

        /**
         * Set a function to be called whenever a message has been received
         * by the server.
         *
         * @param[in] callback
         *     This is the function to call whenever a message has been
         *     received by the server.
         */
        void SetOnReceiveMessageCallback(std::function< void() > callback);

        // IServer
    public:
        virtual EventsUnsubscribeDelegate SubscribeToEvents(EventDelegate eventDelegate) override;
        virtual void Mobilize(
            std::shared_ptr< ILog > logKeeper,
            std::shared_ptr< IPersistentState > persistentStateKeeper,
            std::shared_ptr< Timekeeping::Scheduler > scheduler,
            const ClusterConfiguration& clusterConfiguration,
            const ServerConfiguration& serverConfiguration
        ) override;
        virtual void Demobilize() override;
        virtual void ReceiveMessage(
            const std::string& serializedMessage,
            int senderInstanceNumber
        ) override;
        virtual ElectionState GetElectionState() override;
        virtual void AppendLogEntries(const std::vector< LogEntry >& entries) override;
        virtual void ChangeConfiguration(const ClusterConfiguration& newConfiguration) override;
        virtual void ResetStatistics() override;
        virtual Json::Value GetStatistics() override;

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
        std::shared_ptr< Impl > impl_;
    };

}
