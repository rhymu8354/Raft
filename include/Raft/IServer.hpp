#ifndef RAFT_I_SERVER_HPP
#define RAFT_I_SERVER_HPP

/**
 * @file IServer.hpp
 *
 * This module declares the Raft::IServer interface.
 *
 * © 2018 by Richard Walters
 */

#include <functional>
#include <memory>
#include <ostream>
#include <Raft/Message.hpp>
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
         * This is used to instruct the server on how it should configure
         * itself.
         */
        struct Configuration {
            /**
             * This holds the unique identifiers of all servers in the cluster.
             */
            std::vector< int > instanceNumbers;

            /**
             * This is the unique identifier of this server, amongst all the
             * servers in the cluster.
             */
            int selfInstanceNumber = 0;

            /**
             * This is the last term the server has seen.
             */
            int currentTerm = 0;

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
         * @param[in] message
         *     This is the message to send.
         *
         * @param[in] receiverInstanceNumber
         *     This is the unique identifier of the server to whom to send the
         *     message.
         */
        using SendMessageDelegate = std::function<
            void(
                std::shared_ptr< Message > message,
                int receiverInstanceNumber
            )
        >;

        /**
         * This declares the type of delegate used to create a new message
         * object.
         *
         * @return
         *     A new concrete message object is returned.
         */
        using CreateMessageDelegate = std::function<
            std::shared_ptr< Message >()
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

        // Methods
    public:
        /**
         * This method changes the configuration of the server.
         *
         * @param[in] configuration
         *     This holds the configuration items for the server.
         *
         * @return
         *     An indication of whether or not the configuration
         *     was successfully set is returned.
         */
        virtual bool Configure(const Configuration& configuration) = 0;

        /**
         * This method is called to set up the delegate to be called whenever
         * the server wants to create a message object.
         *
         * @param[in] createMessageDelegate
         *     This is the delegate to be called later whenever the server
         *     wants to create a message object.
         */
        virtual void SetCreateMessageDelegate(CreateMessageDelegate createMessageDelegate) = 0;

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
         * This method starts the server's worker thread.
         */
        virtual void Mobilize() = 0;

        /**
         * This method stops the server's worker thread.
         */
        virtual void Demobilize() = 0;

        /**
         * This method is called whenever the server receives a message
         * from another server in the cluster.
         *
         * @param[in] message
         *     This is the message received from another server in the cluster.
         *
         * @param[in] senderInstanceNumber
         *     This is the unique identifier of the server that sent the
         *     message.
         */
        virtual void ReceiveMessage(
            std::shared_ptr< Message > message,
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
