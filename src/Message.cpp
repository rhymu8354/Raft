/**
 * @file Message.cpp
 *
 * This module contains the implementation of the Raft::Message class.
 *
 * © 2018 by Richard Walters
 */

#include "MessageImpl.hpp"

#include <Raft/Message.hpp>

namespace Raft {

    Message::~Message() noexcept = default;
    Message::Message(Message&&) noexcept = default;
    Message& Message::operator=(Message&&) noexcept = default;

    Message::Message()
        : impl_(new MessageImpl())
    {
    }

    std::string Message::Serialize() {
        return "";
    }

}
