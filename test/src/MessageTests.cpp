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

TEST(MessageTests, SerializeRequestVote) {
    // Arrange
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 42;
    message.requestVote.candidateId = 5;
    message.requestVote.lastLogIndex = 99;

    // Act
    EXPECT_EQ(
        Json::Object({
            {"type", "RequestVote"},
            {"term", 42},
            {"candidateId", 5},
            {"lastLogIndex", 99},
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
    }).ToEncoding();

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::RequestVote, message.type);
    EXPECT_EQ(42, message.requestVote.term);
    EXPECT_EQ(5, message.requestVote.candidateId);
    EXPECT_EQ(11, message.requestVote.lastLogIndex);
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

    // Act
    EXPECT_EQ(
        Json::Object({
            {"type", "AppendEntries"},
            {"term", 8},
            {"leaderCommit", 85},
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
        {"log", Json::Array({})},
    }).ToEncoding();

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::AppendEntries, message.type);
    EXPECT_EQ(8, message.appendEntries.term);
    EXPECT_EQ(18, message.appendEntries.leaderCommit);
}

TEST(MessageTests, SerializeAppendEntriesWithContent) {
    // Arrange
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = 8;
    message.appendEntries.leaderCommit = 77;
    Raft::LogEntry firstEntry;
    firstEntry.term = 7;
    message.log.push_back(std::move(firstEntry));
    Raft::LogEntry secondEntry;
    secondEntry.term = 8;
    message.log.push_back(std::move(secondEntry));

    // Act
    EXPECT_EQ(
        Json::Object({
            {"type", "AppendEntries"},
            {"term", 8},
            {"leaderCommit", 77},
            {"log", Json::Array({
                Json::Object({
                    {"term", 7},
                }),
                Json::Object({
                    {"term", 8},
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
        {"log", Json::Array({
            Json::Object({
                {"term", 7},
            }),
            Json::Object({
                {"term", 8},
            }),
        })},
    }).ToEncoding();

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::AppendEntries, message.type);
    EXPECT_EQ(8, message.appendEntries.term);
    EXPECT_EQ(33, message.appendEntries.leaderCommit);
    ASSERT_EQ(2, message.log.size());
    EXPECT_EQ(7, message.log[0].term);
    EXPECT_EQ(8, message.log[1].term);
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
