/**
 * @file Message.cpp
 *
 * This module contains the implementation of the Raft::Message class.
 *
 * © 2018 by Richard Walters
 */

#include "Message.hpp"

#include <Json/Value.hpp>
#include <Serialization/SerializedBoolean.hpp>
#include <Serialization/SerializedInteger.hpp>
#include <Serialization/SerializedString.hpp>
#include <Serialization/SerializedUnsignedInteger.hpp>
#include <Serialization/SerializedVector.hpp>
#include <SystemAbstractions/StringFile.hpp>

namespace {

    constexpr int CURRENT_SERIALIZATION_VERSION = 1;

}

namespace Raft {

    Message::Message(const std::string& serialization) {
        SystemAbstractions::StringFile buffer(serialization);
        Serialization::SerializedUnsignedInteger version;
        if (!version.Deserialize(&buffer)) {
            return;
        }
        if (version > CURRENT_SERIALIZATION_VERSION) {
            return;
        }
        Serialization::SerializedInteger serializedType;
        if (!serializedType.Deserialize(&buffer)) {
            return;
        }
        type = (Message::Type)(int)serializedType;
        switch (type) {
            case Message::Type::RequestVote: {
                Serialization::SerializedInteger intField;
                if (!intField.Deserialize(&buffer)) {
                    return;
                }
                requestVote.term = intField;
                if (!intField.Deserialize(&buffer)) {
                    return;
                }
                requestVote.candidateId = intField;
                if (!intField.Deserialize(&buffer)) {
                    return;
                }
                requestVote.lastLogIndex = (size_t)intField;
                if (!intField.Deserialize(&buffer)) {
                    return;
                }
                requestVote.lastLogTerm = intField;
            } break;

            case Message::Type::RequestVoteResults: {
                Serialization::SerializedInteger intField;
                if (!intField.Deserialize(&buffer)) {
                    return;
                }
                requestVoteResults.term = intField;
                Serialization::SerializedBoolean boolField;
                if (!boolField.Deserialize(&buffer)) {
                    return;
                }
                requestVoteResults.voteGranted = boolField;
            } break;

            case Message::Type::AppendEntries: {
                Serialization::SerializedInteger intField;
                if (!intField.Deserialize(&buffer)) {
                    return;
                }
                appendEntries.term = intField;
                if (!intField.Deserialize(&buffer)) {
                    return;
                }
                appendEntries.leaderCommit = (size_t)intField;
                if (!intField.Deserialize(&buffer)) {
                    return;
                }
                appendEntries.prevLogIndex = (size_t)intField;
                if (!intField.Deserialize(&buffer)) {
                    return;
                }
                appendEntries.prevLogTerm = intField;
                if (!intField.Deserialize(&buffer)) {
                    return;
                }
                const auto numLogEntries = (size_t)intField;
                log.reserve(numLogEntries);
                for (size_t i = 0; i < numLogEntries; ++i) {
                    LogEntry logEntry;
                    if (!logEntry.Deserialize(&buffer)) {
                        return;
                    }
                    log.push_back(std::move(logEntry));
                }
            } break;

            case Message::Type::AppendEntriesResults: {
                Serialization::SerializedInteger intField;
                if (!intField.Deserialize(&buffer)) {
                    return;
                }
                appendEntriesResults.term = intField;
                Serialization::SerializedBoolean boolField;
                if (!boolField.Deserialize(&buffer)) {
                    return;
                }
                appendEntriesResults.success = boolField;
                if (!intField.Deserialize(&buffer)) {
                    return;
                }
                appendEntriesResults.matchIndex = (size_t)intField;
            } break;

            case Message::Type::InstallSnapshot: {
                Serialization::SerializedInteger intField;
                if (!intField.Deserialize(&buffer)) {
                    return;
                }
                installSnapshot.term = intField;
                if (!intField.Deserialize(&buffer)) {
                    return;
                }
                installSnapshot.lastIncludedIndex = (size_t)intField;
                if (!intField.Deserialize(&buffer)) {
                    return;
                }
                installSnapshot.lastIncludedTerm = intField;
                Serialization::SerializedString stringField;
                if (!stringField.Deserialize(&buffer)) {
                    return;
                }
                snapshot = Json::Value::FromEncoding(stringField);
            } break;

            case Message::Type::InstallSnapshotResults: {
                Serialization::SerializedInteger intField;
                if (!intField.Deserialize(&buffer)) {
                    return;
                }
                installSnapshotResults.term = intField;
                if (!intField.Deserialize(&buffer)) {
                    return;
                }
                installSnapshotResults.matchIndex = (size_t)intField;
            } break;

            default: return;
        }
    }

    std::string Message::Serialize() const {
        SystemAbstractions::StringFile buffer;
        Serialization::SerializedInteger version(CURRENT_SERIALIZATION_VERSION);
        if (!version.Serialize(&buffer)) {
            return "";
        }
        Serialization::SerializedInteger serializedType((int)type);
        if (!serializedType.Serialize(&buffer)) {
            return "";
        }
        switch (type) {
            case Message::Type::RequestVote: {
                Serialization::SerializedInteger intField(requestVote.term);
                if (!intField.Serialize(&buffer)) {
                    return "";
                }
                intField = requestVote.candidateId;
                if (!intField.Serialize(&buffer)) {
                    return "";
                }
                intField = (int)requestVote.lastLogIndex;
                if (!intField.Serialize(&buffer)) {
                    return "";
                }
                intField = requestVote.lastLogTerm;
                if (!intField.Serialize(&buffer)) {
                    return "";
                }
            } break;

            case Message::Type::RequestVoteResults: {
                Serialization::SerializedInteger intField(requestVoteResults.term);
                if (!intField.Serialize(&buffer)) {
                    return "";
                }
                Serialization::SerializedInteger boolField(requestVoteResults.voteGranted);
                if (!boolField.Serialize(&buffer)) {
                    return "";
                }
            } break;

            case Message::Type::AppendEntries: {
                Serialization::SerializedInteger intField(appendEntries.term);
                if (!intField.Serialize(&buffer)) {
                    return "";
                }
                intField = (int)appendEntries.leaderCommit;
                if (!intField.Serialize(&buffer)) {
                    return "";
                }
                intField = (int)appendEntries.prevLogIndex;
                if (!intField.Serialize(&buffer)) {
                    return "";
                }
                intField = appendEntries.prevLogTerm;
                if (!intField.Serialize(&buffer)) {
                    return "";
                }
                intField = (int)log.size();
                if (!intField.Serialize(&buffer)) {
                    return "";
                }
                for (const auto& logEntry: log) {
                    if (!logEntry.Serialize(&buffer)) {
                        return "";
                    }
                }
            } break;

            case Message::Type::AppendEntriesResults: {
                Serialization::SerializedInteger intField(appendEntriesResults.term);
                if (!intField.Serialize(&buffer)) {
                    return "";
                }
                Serialization::SerializedInteger boolField(appendEntriesResults.success);
                if (!boolField.Serialize(&buffer)) {
                    return "";
                }
                intField = (int)appendEntriesResults.matchIndex;
                if (!intField.Serialize(&buffer)) {
                    return "";
                }
            } break;

            case Message::Type::InstallSnapshot: {
                Serialization::SerializedInteger intField(installSnapshot.term);
                if (!intField.Serialize(&buffer)) {
                    return "";
                }
                intField = (int)installSnapshot.lastIncludedIndex;
                if (!intField.Serialize(&buffer)) {
                    return "";
                }
                intField = installSnapshot.lastIncludedTerm;
                if (!intField.Serialize(&buffer)) {
                    return "";
                }
                Serialization::SerializedString stringField(snapshot.ToEncoding());
                if (!stringField.Serialize(&buffer)) {
                    return "";
                }
            } break;

            case Message::Type::InstallSnapshotResults: {
                Serialization::SerializedInteger intField(installSnapshotResults.term);
                if (!intField.Serialize(&buffer)) {
                    return "";
                }
                intField = (int)installSnapshotResults.matchIndex;
                if (!intField.Serialize(&buffer)) {
                    return "";
                }
            } break;

            default: return "";
        }
        return buffer;
    }

}
