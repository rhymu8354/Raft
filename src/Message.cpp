/**
 * @file Message.cpp
 *
 * This module contains the implementation of the Raft::Message class.
 *
 * © 2018 by Richard Walters
 */

#include "MessageImpl.hpp"

#include <Raft/Message.hpp>

namespace {

    std::shared_ptr< Raft::Message > CreateBaseMessage() {
        return std::make_shared< Raft::Message >();
    }

}

namespace Raft {

    Message::~Message() noexcept = default;
    Message::Message(Message&&) noexcept = default;
    Message& Message::operator=(Message&&) noexcept = default;

    std::function< std::shared_ptr< Message >() > Message::CreateMessage = CreateBaseMessage;

    Message::Message()
        : impl_(new MessageImpl())
    {
    }

    std::string Message::Serialize() {
        return "";
    }

}
