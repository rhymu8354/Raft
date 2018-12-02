/**
 * @file Message.cpp
 *
 * This module contains the implementation of the Raft::Message class.
 *
 * © 2018 by Richard Walters
 */

#include <Raft/Message.hpp>

namespace Raft {

    /**
     * This contains the private properties of a Message class instance
     * that don't live any longer than the Message class instance itself.
     */
    struct Message::Impl {
        bool isElectionMessage = false;
    };

    Message::~Message() noexcept = default;
    Message::Message(Message&&) noexcept = default;
    Message& Message::operator=(Message&&) noexcept = default;

    Message::Message()
        : impl_(new Impl())
    {
    }

    bool Message::IsElectionMessage() const {
        return impl_->isElectionMessage;
    }

    void Message::FormElectionMessage() {
        impl_->isElectionMessage = true;
    }

}
