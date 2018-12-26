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

    Message::Message(const std::string& serialization)
        : impl_(new MessageImpl())
    {
        const auto json = Json::Value::FromEncoding(serialization);
        const auto type = (std::string)json["type"];
        if (type == "RequestVote") {
            impl_->type = MessageImpl::Type::RequestVote;
            impl_->requestVote.term = json["term"];
            impl_->requestVote.candidateId = json["candidateId"];
        } else if (type == "RequestVoteResults") {
            impl_->type = MessageImpl::Type::RequestVoteResults;
            impl_->requestVoteResults.term = json["term"];
            impl_->requestVoteResults.voteGranted = json["voteGranted"];
        } else if (type == "AppendEntries") {
            impl_->type = MessageImpl::Type::AppendEntries;
            impl_->appendEntries.term = json["term"];
            const auto& serializedLogEntries = json["log"];
            for (size_t i = 0; i < serializedLogEntries.GetSize(); ++i) {
                LogEntry logEntry;
                logEntry.term = serializedLogEntries[i]["term"];
                impl_->log.push_back(std::move(logEntry));
            }
        }
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

            case MessageImpl::Type::AppendEntries: {
                json["type"] = "AppendEntries";
                json["term"] = (int)impl_->appendEntries.term;
                json["log"] = Json::Array({});
                auto& serializedLog = json["log"];
                for (const auto& logEntry: impl_->log) {
                    auto serializedLogEntry = Json::Object({
                        {"term", logEntry.term},
                    });
                    serializedLog.Add(std::move(serializedLogEntry));
                }
            } break;

            case MessageImpl::Type::AppendEntriesResults: {
            } break;

            default: {
            } break;
        }
        return json.ToEncoding();
    }

}
