#ifndef RAFT_LOG_MESSAGE_HPP
#define RAFT_LOG_MESSAGE_HPP

/**
 * @file Message.hpp
 *
 * This module declares the Raft::Message implementation.
 *
 * © 2018 by Richard Walters
 */

#include <memory>

namespace Raft {

    /**
     * This is the base class for a message sent from one Message to another
     * within the cluster.
     */
    class Message {
        // Lifecycle Methods
    public:
        ~Message() noexcept;
        Message(const Message&) = delete;
        Message(Message&&) noexcept;
        Message& operator=(const Message&) = delete;
        Message& operator=(Message&&) noexcept;

        // Public Methods
    public:
        /**
         * This is the constructor of the class.
         */
        Message();

        /**
         * This method returns an indication of whether or not the message is
         * meant to start a cluster leader election.
         *
         * @return
         *     An indication of whether or not the message is
         *     meant to start a cluster leader election is returned.
         */
        bool IsElectionMessage() const;

        /**
         * This method forms the message to be a server cluster election
         * message.
         */
        void FormElectionMessage();

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

#endif /* RAFT_LOG_ENTRY_HPP */
