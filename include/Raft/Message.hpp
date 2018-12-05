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
#include <string>

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
         *
         * @param[in] serialization
         *     If not empty, this is the serialized form of the message, used
         *     to initialize the type and properties of the message.
         */
        Message(const std::string& serialization = "");

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
