#ifndef RAFT_LOG_MESSAGE_HPP
#define RAFT_LOG_MESSAGE_HPP

/**
 * @file Message.hpp
 *
 * This module declares the Raft::Message implementation.
 *
 * © 2018 by Richard Walters
 */

#include <functional>
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

        // Public Class Properties
    public:
        /**
         * This is the factory function called by the Message class internally
         * to create a new message.
         *
         * @note
         *     Subclasses are expected to replace this with a function which
         *     will create a subclass of Message, not the base Message alone.
         *     Otherwise the messages created by the Raft::Server
         *     implementation may not have all the expected functionality
         *     needed by the concrete server.
         *
         * @return
         *     A new concrete message object is returned.
         */
        static std::function< std::shared_ptr< Message >() > CreateMessage;

        // Public Methods
    public:
        /**
         * This is the constructor of the class.
         */
        Message();

        /**
         * This method returns a string which can be used to construct a new
         * message with the exact same contents as this message.
         *
         * @return
         *     A string which can be used to construct a new message with the
         *     exact same contents as this message is returned.
         */
        virtual std::string Serialize();

        // Package-Private properties (public but opaque)
    public:
        /**
         * This contains the package-private properties of the instance.
         */
        std::unique_ptr< struct MessageImpl > impl_;
    };

}

#endif /* RAFT_LOG_ENTRY_HPP */
