#ifndef RAFT_I_SERVER_HPP
#define RAFT_I_SERVER_HPP

/**
 * @file IServer.hpp
 *
 * This module declares the Raft::IServer interface.
 *
 * © 2018 by Richard Walters
 */

#include "ClusterConfiguration.hpp"
#include "ILog.hpp"
#include "IPersistentState.hpp"
#include "LogEntry.hpp"

#include <functional>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <vector>

namespace Raft {

    /**
     * This is the interface to the Server component, which represents one
     * member of the server cluster.
     */
    class IServer {
        // Types
    public:
        /**
         * This is used to indicate whether the server is a currently a leader,
         * candidate, or follower in the current election term of the cluster.
         */
        enum class ElectionState {
            /**
             * In this state, the server is neither a leader, or is running for
             * election in the current term.  It is awaiting heartbeat messages
             * from the leader, and if none is received before the election
             * timeout, it will start a new election.
             */
            Follower,

            /**
             * In this state, the server is running for election in the current
             * term, awaiting vote responses.  If it receives a majority vote,
             * it will immediately become the leader.  If it receives a
             * heartbeat, it will immediately become a follower.  Otherwise, it
             * will start a new election once the election timeout occurs.
             */
            Candidate,

            /**
             * In this state, the server is the leader of the cluster in the
             * current term, and will send out heartbeats to all the other
             * servers.  It reverts to a follower if it receives a heartbeat or
             * vote request in a newer term.
             */
            Leader,
        };

        /**
         * This holds the properties which make up the configuration for just
         * this server and not any other servers in the cluster.
         */
        struct ServerConfiguration {
            /**
             * This is the unique identifier of this server, amongst all the
             * servers in the cluster.
             */
            int selfInstanceId = 0;

            /**
             * This is the lower bound of the range of time, starting from the
             * last time the server either started or received a message from
             * the cluster leader, within which to trigger an election.
             */
            double minimumElectionTimeout = 0.15;

            /**
             * This is the upper bound of the range of time, starting from the
             * last time the server either started or received a message from
             * the cluster leader, within which to trigger an election.
             */
            double maximumElectionTimeout = 0.3;

            /**
             * This is the maximum amount of time to wait for a response to an
             * RPC request, before retransmitting the request.
             */
            double rpcTimeout = 0.015;
        };

        /**
         * This declares the type of delegate used to request that a message
         * be sent to another server in the cluster.
         *
         * @param[in] serializedMessage
         *     This is the serialized message to send.
         *
         * @param[in] receiverInstanceNumber
         *     This is the unique identifier of the server to whom to send the
         *     message.
         */
        using SendMessageDelegate = std::function<
            void(
                const std::string& serializedMessage,
                int receiverInstanceNumber
            )
        >;

        /**
         * This declares the type of delegate used to announce leadership
         * changes in the server cluster.
         *
         * @param[in] leaderId
         *     This is the unique identifier of the server which has become
         *     the leader of the cluster.
         *
         * @param[in] term
         *     This is the generation number of the server cluster leadership,
         *     which is incremented whenever a new election is started.
         */
        using LeadershipChangeDelegate = std::function<
            void(
                int leaderId,
                int term
            )
        >;

        /**
         * Declare the type of delegate used to announce that a single cluster
         * configuration has been applied by the server.
         *
         * @param[in] newConfig
         *     This is the new single cluster configuration applied by
         *     the server.
         */
        using ApplyConfigurationDelegate = std::function<
            void(
                const ClusterConfiguration& newConfig
            )
        >;

        // Methods
    public:
        /**
         * This method is called to set up the delegate to be called later
         * whenever the server wants to send a message to another server in the
         * cluster.
         *
         * @param[in] sendMessageDelegate
         *     This is the delegate to be called later whenever the server
         *     wants to send a message to another server in the cluster.
         */
        virtual void SetSendMessageDelegate(SendMessageDelegate sendMessageDelegate) = 0;

        /**
         * This method is called to set up the delegate to be called later
         * whenever a leadership change occurs in the server cluster.
         *
         * @param[in] leadershipChangeDelegate
         *     This is the delegate to be called later whenever a leadership
         *     change occurs in the server cluster.
         */
        virtual void SetLeadershipChangeDelegate(LeadershipChangeDelegate leadershipChangeDelegate) = 0;

        /**
         * Set up a delegate to be called later whenever a single cluster
         * configuration is applied by the server.
         *
         * @param[in] applyConfigurationDelegate
         *     This is the delegate to be called later whenever a single
         *     cluster configuration is applied by the server.
         */
        virtual void SetApplyConfigurationDelegate(ApplyConfigurationDelegate applyConfigurationDelegate) = 0;

        /**
         * This method starts the server's worker thread.
         *
         * @param[in] logKeeper
         *     This is the object which is responsible for keeping
         *     the actual log and making it persistent.
         *
         * @param[in] persistentState
         *     This is the object which is responsible for keeping
         *     the server state variables which need to be persistent.
         *
         * @param[in] clusterConfiguration
         *     This holds the configuration items for the cluster.
         *
         * @param[in] serverConfiguration
         *     This holds the configuration items for the server.
         */
        virtual void Mobilize(
            std::shared_ptr< ILog > logKeeper,
            std::shared_ptr< IPersistentState > persistentStateKeeper,
            const ClusterConfiguration& clusterConfiguration,
            const ServerConfiguration& serverConfiguration
        ) = 0;

        /**
         * This method stops the server's worker thread.
         */
        virtual void Demobilize() = 0;

        /**
         * This method is called whenever the server receives a message
         * from another server in the cluster.
         *
         * @param[in] serializedMessage
         *     This is the serialized message received from another server
         *     in the cluster.
         *
         * @param[in] senderInstanceNumber
         *     This is the unique identifier of the server that sent the
         *     message.
         */
        virtual void ReceiveMessage(
            const std::string& serializedMessage,
            int senderInstanceNumber
        ) = 0;

        /**
         * This method returns an indication of whether the server is currently
         * the leader, a candidate, or a follower in the current term of the
         * cluster.
         *
         * @return
         *     An indication of whether the server is currently the leader,
         *     a candidate, or a follower in the current term of the cluster
         *     is returned.
         */
        virtual ElectionState GetElectionState() = 0;

        /**
         * Add the given entries to the server log.  Send out AppendEntries
         * messages to the rest of the server cluster in order to replicate the
         * log entries.
         *
         * @param[in] entries
         *     These are the log entries to be added and replicated on all
         *     servers in the cluster.
         */
        virtual void AppendLogEntries(const std::vector< LogEntry >& entries) = 0;

        /**
         * Begin the process of reconfiguring the server cluster.  The server
         * is expected to start sending log entries to the union of servers in
         * both the current and new configurations.  Once all "new servers"
         * have "caught up", the server will go through the joint configuration
         * process to transition to the new configuration.
         *
         * @param[in] newConfiguration
         *     This represents the cluster shape to which we want to
         *     transition.
         */
        virtual void ChangeConfiguration(const ClusterConfiguration& newConfiguration) = 0;
    };

    /**
     * This is a support function for Google Test to print out
     * values of the Raft::IServer::ElectionState class.
     *
     * @param[in] electionState
     *     This is the Raft election state value to print.
     *
     * @param[in] os
     *     This points to the stream to which to print the
     *     Raft election state value.
     */
    void PrintTo(
        const Raft::IServer::ElectionState& electionState,
        std::ostream* os
    );

}

#endif /* RAFT_I_SERVER_HPP */
