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

TEST(MessageTests, SerializeHeartBeat) {
    // Arrange
    const auto message = std::make_shared < Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::HeartBeat;
    message->impl_->heartbeat.term = 8;

    // Act
    EXPECT_EQ(
        Json::Object({
            {"type", "HeartBeat"},
            {"term", 8},
        }),
        Json::Value::FromEncoding(message->Serialize())
    );
}
