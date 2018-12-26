/**
 * @file MessageTests.cpp
 *
 * This module contains the unit tests of the
 * Raft::Message class.
 *
 * © 2018 by Richard Walters
 */

#include <src/MessageImpl.hpp>

#include <gtest/gtest.h>
#include <Json/Value.hpp>
#include <Raft/Message.hpp>

TEST(MessageTests, SerializeRequestVote) {
    // Arrange
    const auto message = std::make_shared < Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::RequestVote;
    message->impl_->requestVote.term = 42;
    message->impl_->requestVote.candidateId = 5;

    // Act
    EXPECT_EQ(
        Json::Object({
            {"type", "RequestVote"},
            {"term", 42},
            {"candidateId", 5}
        }),
        Json::Value::FromEncoding(message->Serialize())
    );
}

TEST(MessageTests, DeserializeRequestVote) {
    // Arrange
    const auto serializedMessage = Json::Object({
        {"type", "RequestVote"},
        {"term", 42},
        {"candidateId", 5}
    }).ToEncoding();

    // Act
    const auto message = std::make_shared < Raft::Message >(serializedMessage);

    // Act
    EXPECT_EQ(Raft::MessageImpl::Type::RequestVote, message->impl_->type);
    EXPECT_EQ(42, message->impl_->requestVote.term);
    EXPECT_EQ(5, message->impl_->requestVote.candidateId);
}

TEST(MessageTests, SerializeRequestVoteResponse) {
    // Arrange
    const auto message = std::make_shared < Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
    message->impl_->requestVoteResults.term = 16;
    message->impl_->requestVoteResults.voteGranted = true;

    // Act
    EXPECT_EQ(
        Json::Object({
            {"type", "RequestVoteResults"},
            {"term", 16},
            {"voteGranted", true}
        }),
        Json::Value::FromEncoding(message->Serialize())
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
    const auto message = std::make_shared < Raft::Message >(serializedMessage);

    // Act
    EXPECT_EQ(Raft::MessageImpl::Type::RequestVoteResults, message->impl_->type);
    EXPECT_EQ(16, message->impl_->requestVoteResults.term);
    EXPECT_TRUE(message->impl_->requestVoteResults.voteGranted);
}

TEST(MessageTests, SerializeHeartBeat) {
    // Arrange
    const auto message = std::make_shared < Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
    message->impl_->appendEntries.term = 8;
    message->impl_->appendEntries.leaderCommit = 85;

    // Act
    EXPECT_EQ(
        Json::Object({
            {"type", "AppendEntries"},
            {"term", 8},
            {"leaderCommit", 85},
            {"log", Json::Array({})},
        }),
        Json::Value::FromEncoding(message->Serialize())
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
    const auto message = std::make_shared < Raft::Message >(serializedMessage);

    // Act
    EXPECT_EQ(Raft::MessageImpl::Type::AppendEntries, message->impl_->type);
    EXPECT_EQ(8, message->impl_->appendEntries.term);
    EXPECT_EQ(18, message->impl_->appendEntries.leaderCommit);
}

TEST(MessageTests, SerializeAppendEntriesWithContent) {
    // Arrange
    const auto message = std::make_shared < Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
    message->impl_->appendEntries.term = 8;
    message->impl_->appendEntries.leaderCommit = 77;
    Raft::LogEntry firstEntry;
    firstEntry.term = 7;
    message->impl_->log.push_back(std::move(firstEntry));
    Raft::LogEntry secondEntry;
    secondEntry.term = 8;
    message->impl_->log.push_back(std::move(secondEntry));

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
        Json::Value::FromEncoding(message->Serialize())
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
    const auto message = std::make_shared < Raft::Message >(serializedMessage);

    // Act
    EXPECT_EQ(Raft::MessageImpl::Type::AppendEntries, message->impl_->type);
    EXPECT_EQ(8, message->impl_->appendEntries.term);
    EXPECT_EQ(33, message->impl_->appendEntries.leaderCommit);
    ASSERT_EQ(2, message->impl_->log.size());
    EXPECT_EQ(7, message->impl_->log[0].term);
    EXPECT_EQ(8, message->impl_->log[1].term);
}

TEST(MessageTests, SerializeUnknown) {
    // Arrange
    const auto message = std::make_shared < Raft::Message >();

    // Act
    EXPECT_EQ(
        Json::Object({}),
        Json::Value::FromEncoding(message->Serialize())
    );
}

TEST(MessageTests, DeserializeGarbage) {
    // Arrange
    const auto serializedMessage = "admLUL PogChamp";

    // Act
    const auto message = std::make_shared < Raft::Message >(serializedMessage);

    // Act
    EXPECT_EQ(Raft::MessageImpl::Type::Unknown, message->impl_->type);
}
