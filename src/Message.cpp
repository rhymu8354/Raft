/**
 * @file Message.cpp
 *
 * This module contains the implementation of the Raft::Message class.
 *
 * © 2018 by Richard Walters
 */

#include "MessageImpl.hpp"

#include <Json/Value.hpp>
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
        auto json = Json::Object({});
        switch (impl_->type) {
            case MessageImpl::Type::RequestVote: {
                json["type"] = "RequestVote";
                json["term"] = (int)impl_->requestVote.term;
                json["candidateId"] = (int)impl_->requestVote.candidateId;
            } break;

            case MessageImpl::Type::RequestVoteResults: {
                json["type"] = "RequestVoteResults";
                json["term"] = (int)impl_->requestVoteResults.term;
                json["voteGranted"] = impl_->requestVoteResults.voteGranted;
            } break;

            case MessageImpl::Type::HeartBeat: {
                json["type"] = "HeartBeat";
                json["term"] = (int)impl_->heartbeat.term;
            } break;

            default: {
            } break;
        }
        return json.ToEncoding();
    }

}
