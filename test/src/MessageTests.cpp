/**
 * @file MessageTests.cpp
 *
 * This module contains the unit tests of the
 * Raft::Message class.
 *
 * © 2018 by Richard Walters
 */

#include "../../src/Message.hpp"

#include <gtest/gtest.h>
#include <Json/Value.hpp>
#include <Raft/LogEntry.hpp>

TEST(MessageTests, SerializeRequestVote) {
    // Arrange
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 42;
    message.requestVote.candidateId = 5;
    message.requestVote.lastLogIndex = 99;
    message.requestVote.lastLogTerm = 7;

    // Act
    EXPECT_EQ(
        Json::Object({
            {"type", "RequestVote"},
            {"term", 42},
            {"candidateId", 5},
            {"lastLogIndex", 99},
            {"lastLogTerm", 7},
        }),
        Json::Value::FromEncoding(message.Serialize())
    );
}

TEST(MessageTests, DeserializeRequestVote) {
    // Arrange
    const auto serializedMessage = Json::Object({
        {"type", "RequestVote"},
        {"term", 42},
        {"candidateId", 5},
        {"lastLogIndex", 11},
        {"lastLogTerm", 3},
    }).ToEncoding();

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::RequestVote, message.type);
    EXPECT_EQ(42, message.requestVote.term);
    EXPECT_EQ(5, message.requestVote.candidateId);
    EXPECT_EQ(11, message.requestVote.lastLogIndex);
    EXPECT_EQ(3, message.requestVote.lastLogTerm);
}

TEST(MessageTests, SerializeRequestVoteResponse) {
    // Arrange
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVoteResults;
    message.requestVoteResults.term = 16;
    message.requestVoteResults.voteGranted = true;

    // Act
    EXPECT_EQ(
        Json::Object({
            {"type", "RequestVoteResults"},
            {"term", 16},
            {"voteGranted", true}
        }),
        Json::Value::FromEncoding(message.Serialize())
    );
}

TEST(MessageTests, DeserializeRequestVoteResults) {
    // Arrange
    const auto serializedMessage = Json::Object({
        {"type", "RequestVoteResults"},
        {"term", 16},
        {"voteGranted", true}
    }).ToEncoding();

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::RequestVoteResults, message.type);
    EXPECT_EQ(16, message.requestVoteResults.term);
    EXPECT_TRUE(message.requestVoteResults.voteGranted);
}

TEST(MessageTests, SerializeHeartBeat) {
    // Arrange
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = 8;
    message.appendEntries.leaderCommit = 85;
    message.appendEntries.prevLogIndex = 2;
    message.appendEntries.prevLogTerm = 7;

    // Act
    EXPECT_EQ(
        Json::Object({
            {"type", "AppendEntries"},
            {"term", 8},
            {"leaderCommit", 85},
            {"prevLogIndex", 2},
            {"prevLogTerm", 7},
            {"log", Json::Array({})},
        }),
        Json::Value::FromEncoding(message.Serialize())
    );
}

TEST(MessageTests, DeserializeHeartBeat) {
    // Arrange
    const auto serializedMessage = Json::Object({
        {"type", "AppendEntries"},
        {"term", 8},
        {"leaderCommit", 18},
        {"prevLogIndex", 6},
        {"prevLogTerm", 1},
        {"log", Json::Array({})},
    }).ToEncoding();

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::AppendEntries, message.type);
    EXPECT_EQ(8, message.appendEntries.term);
    EXPECT_EQ(18, message.appendEntries.leaderCommit);
    EXPECT_EQ(6, message.appendEntries.prevLogIndex);
    EXPECT_EQ(1, message.appendEntries.prevLogTerm);
}

TEST(MessageTests, SerializeAppendEntriesWithContent) {
    // Arrange
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = 8;
    message.appendEntries.leaderCommit = 77;
    message.appendEntries.prevLogIndex = 2;
    message.appendEntries.prevLogTerm = 7;
    Raft::LogEntry firstEntry;
    firstEntry.term = 7;
    message.log.push_back(std::move(firstEntry));
    Raft::LogEntry secondEntry;
    secondEntry.term = 8;
    auto command = std::make_shared< Raft::SingleConfigurationCommand >();
    command->oldConfiguration.instanceIds = {2, 5, 6, 7, 11};
    command->configuration.instanceIds = {2, 5, 6, 7, 12};
    secondEntry.command = std::move(command);
    message.log.push_back(std::move(secondEntry));

    // Act
    EXPECT_EQ(
        Json::Object({
            {"type", "AppendEntries"},
            {"term", 8},
            {"leaderCommit", 77},
            {"prevLogIndex", 2},
            {"prevLogTerm", 7},
            {"log", Json::Array({
                Json::Object({
                    {"term", 7},
                }),
                Json::Object({
                    {"term", 8},
                    {"type", "SingleConfiguration"},
                    {"command", Json::Object({
                        {"oldConfiguration", Json::Object({
                            {"instanceIds", Json::Array({2, 5, 6, 7, 11})},
                        })},
                        {"configuration", Json::Object({
                            {"instanceIds", Json::Array({2, 5, 6, 7, 12})},
                        })},
                    })},
                }),
            })},
        }),
        Json::Value::FromEncoding(message.Serialize())
    );
}

TEST(MessageTests, DeserializeAppendEntriesWithContent) {
    // Arrange
    const auto serializedMessage = Json::Object({
        {"type", "AppendEntries"},
        {"term", 8},
        {"leaderCommit", 33},
        {"prevLogIndex", 5},
        {"prevLogTerm", 6},
        {"log", Json::Array({
            Json::Object({
                {"term", 7},
            }),
            Json::Object({
                {"term", 8},
                {"type", "SingleConfiguration"},
                {"command", Json::Object({
                    {"oldConfiguration", Json::Object({
                        {"instanceIds", Json::Array({2, 5, 6, 7, 11})},
                    })},
                    {"configuration", Json::Object({
                        {"instanceIds", Json::Array({2, 5, 6, 7, 12})},
                    })},
                })},
            }),
        })},
    }).ToEncoding();

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::AppendEntries, message.type);
    EXPECT_EQ(8, message.appendEntries.term);
    EXPECT_EQ(33, message.appendEntries.leaderCommit);
    EXPECT_EQ(5, message.appendEntries.prevLogIndex);
    EXPECT_EQ(6, message.appendEntries.prevLogTerm);
    ASSERT_EQ(2, message.log.size());
    EXPECT_EQ(7, message.log[0].term);
    EXPECT_TRUE(message.log[0].command == nullptr);
    EXPECT_EQ(8, message.log[1].term);
    ASSERT_FALSE(message.log[1].command == nullptr);
    ASSERT_EQ("SingleConfiguration", message.log[1].command->GetType());
    const auto singleConfigurationCommand = std::static_pointer_cast< Raft::SingleConfigurationCommand >(message.log[1].command);
    EXPECT_EQ(
        std::set< int >({2, 5, 6, 7, 11}),
        singleConfigurationCommand->oldConfiguration.instanceIds
    );
    EXPECT_EQ(
        std::set< int >({2, 5, 6, 7, 12}),
        singleConfigurationCommand->configuration.instanceIds
    );
}

TEST(MessageTests, SerializeUnknown) {
    // Arrange
    Raft::Message message;

    // Act
    EXPECT_EQ(
        Json::Object({}),
        Json::Value::FromEncoding(message.Serialize())
    );
}

TEST(MessageTests, DeserializeGarbage) {
    // Arrange
    const auto serializedMessage = "admLUL PogChamp";

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::Unknown, message.type);
}

TEST(MessageTests, SerializeAppendEntriesResults) {
    // Arrange
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntriesResults;
    message.appendEntriesResults.term = 8;
    message.appendEntriesResults.success = true;
    message.appendEntriesResults.matchIndex = 5;

    // Act
    EXPECT_EQ(
        Json::Object({
            {"type", "AppendEntriesResults"},
            {"term", 8},
            {"matchIndex", 5},
            {"success", true},
        }),
        Json::Value::FromEncoding(message.Serialize())
    );
}

TEST(MessageTests, DeserializeAppendEntriesResults) {
    // Arrange
    const auto serializedMessage = Json::Object({
        {"type", "AppendEntriesResults"},
        {"term", 5},
        {"matchIndex", 10},
        {"success", false},
    }).ToEncoding();

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, message.type);
    EXPECT_EQ(5, message.appendEntriesResults.term);
    EXPECT_FALSE(message.appendEntriesResults.success);
    EXPECT_EQ(10, message.appendEntriesResults.matchIndex);
}

TEST(MessageTests, SerializeInstallSnapshot) {
    // Arrange
    Raft::Message message;
    message.type = Raft::Message::Type::InstallSnapshot;
    message.installSnapshot.term = 8;
    message.installSnapshot.lastIncludedIndex = 2;
    message.installSnapshot.lastIncludedTerm = 7;
    message.snapshot = Json::Object({
        {"foo", "bar"},
    });

    // Act
    EXPECT_EQ(
        Json::Object({
            {"type", "InstallSnapshot"},
            {"term", 8},
            {"lastIncludedIndex", 2},
            {"lastIncludedTerm", 7},
            {"snapshot", message.snapshot},
        }),
        Json::Value::FromEncoding(message.Serialize())
    );
}

TEST(MessageTests, DeserializeInstallSnapshot) {
    // Arrange
    const Json::Value snapshot = Json::Object({
        {"foo", "bar"},
    });
    const auto serializedMessage = Json::Object({
        {"type", "InstallSnapshot"},
        {"term", 8},
        {"lastIncludedIndex", 2},
        {"lastIncludedTerm", 7},
        {"snapshot", snapshot},
    }).ToEncoding();

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::InstallSnapshot, message.type);
    EXPECT_EQ(8, message.installSnapshot.term);
    EXPECT_EQ(2, message.installSnapshot.lastIncludedIndex);
    EXPECT_EQ(7, message.installSnapshot.lastIncludedTerm);
    EXPECT_EQ(snapshot, message.snapshot);
}

TEST(MessageTests, SerializeInstallSnapshotResults) {
    // Arrange
    Raft::Message message;
    message.type = Raft::Message::Type::InstallSnapshotResults;
    message.installSnapshotResults.term = 8;
    message.installSnapshotResults.matchIndex = 100;

    // Act
    EXPECT_EQ(
        Json::Object({
            {"type", "InstallSnapshotResults"},
            {"term", 8},
            {"matchIndex", 100},
        }),
        Json::Value::FromEncoding(message.Serialize())
    );
}

TEST(MessageTests, DeserializeInstallSnapshotResults) {
    // Arrange
    const auto serializedMessage = Json::Object({
        {"type", "InstallSnapshotResults"},
        {"term", 8},
        {"matchIndex", 100},
    }).ToEncoding();

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::InstallSnapshotResults, message.type);
    EXPECT_EQ(8, message.installSnapshotResults.term);
    EXPECT_EQ(100, message.installSnapshotResults.matchIndex);
}
