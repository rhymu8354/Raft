/**
 * @file Message.cpp
 *
 * This module contains the implementation of the Raft::Message class.
 *
 * © 2018 by Richard Walters
 */

#include "Message.hpp"

#include <Json/Value.hpp>

namespace Raft {

    Message::Message(const std::string& serialization) {
        const auto json = Json::Value::FromEncoding(serialization);
        const std::string typeAsString = json["type"];
        if (typeAsString == "RequestVote") {
            type = Message::Type::RequestVote;
            requestVote.term = json["term"];
            requestVote.candidateId = json["candidateId"];
            requestVote.lastLogIndex = json["lastLogIndex"];
        } else if (typeAsString == "RequestVoteResults") {
            type = Message::Type::RequestVoteResults;
            requestVoteResults.term = json["term"];
            requestVoteResults.voteGranted = json["voteGranted"];
        } else if (typeAsString == "AppendEntries") {
            type = Message::Type::AppendEntries;
            appendEntries.term = json["term"];
            appendEntries.leaderCommit = json["leaderCommit"];
            const auto& serializedLogEntries = json["log"];
            for (size_t i = 0; i < serializedLogEntries.GetSize(); ++i) {
                LogEntry logEntry;
                logEntry.term = serializedLogEntries[i]["term"];
                log.push_back(std::move(logEntry));
            }
        } else if (typeAsString == "AppendEntriesResults") {
            type = Message::Type::AppendEntriesResults;
        }
    }

    std::string Message::Serialize() const {
        auto json = Json::Object({});
        switch (type) {
            case Message::Type::RequestVote: {
                json["type"] = "RequestVote";
                json["term"] = requestVote.term;
                json["candidateId"] = requestVote.candidateId;
                json["lastLogIndex"] = requestVote.lastLogIndex;
            } break;

            case Message::Type::RequestVoteResults: {
                json["type"] = "RequestVoteResults";
                json["term"] = requestVoteResults.term;
                json["voteGranted"] = requestVoteResults.voteGranted;
            } break;

            case Message::Type::AppendEntries: {
                json["type"] = "AppendEntries";
                json["term"] = appendEntries.term;
                json["leaderCommit"] = appendEntries.leaderCommit;
                json["log"] = Json::Array({});
                auto& serializedLog = json["log"];
                for (const auto& logEntry: log) {
                    auto serializedLogEntry = Json::Object({
                        {"term", logEntry.term},
                    });
                    serializedLog.Add(std::move(serializedLogEntry));
                }
            } break;

            case Message::Type::AppendEntriesResults: {
                json["type"] = "AppendEntriesResults";
            } break;

            default: {
            } break;
        }
        return json.ToEncoding();
    }

}
