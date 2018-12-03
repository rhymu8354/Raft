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
         * This is used to instruct the server on how it should configure
         * itself.
         */
        struct Configuration {
            /**
             * This holds the unique identifiers of all servers in the cluster.
             */
            std::vector< unsigned int > instanceNumbers;

            /**
             * This is the unique identifier of this server, amongst all the
             * servers in the cluster.
             */
            unsigned int selfInstanceNumber = 0;

            /**
             * This is the last term the server has seen.
             */
            unsigned int currentTerm = 0;

            /**
             * This is the lower bound of the range of time, starting from the
             * last time the server either started or received a message from
             * the cluster leader, within which to trigger an election.
             */
            double minimumTimeout = 0.15;

            /**
             * This is the upper bound of the range of time, starting from the
             * last time the server either started or received a message from
             * the cluster leader, within which to trigger an election.
             */
            double maximumTimeout = 0.3;
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
                unsigned int receiverInstanceNumber
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
            unsigned int senderInstanceNumber
        ) = 0;

        /**
         * This method returns an indication of whether or not the server is
         * currently the leader of the cluster.
         *
         * @return
         *     An indication of whether or not the server is
         *     currently the leader of the cluster is returned.
         */
        virtual bool IsLeader() = 0;
    };

}

#endif /* RAFT_I_SERVER_HPP */
