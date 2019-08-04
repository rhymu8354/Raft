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

TEST(MessageTests, RequestVote) {
    // Arrange
    Raft::Message messageIn;
    messageIn.type = Raft::Message::Type::RequestVote;
    messageIn.term = 42;
    messageIn.seq = 7;
    messageIn.requestVote.candidateId = 5;
    messageIn.requestVote.lastLogIndex = 11;
    messageIn.requestVote.lastLogTerm = 3;
    const auto serializedMessage = messageIn.Serialize();

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::RequestVote, message.type);
    EXPECT_EQ(42, message.term);
    EXPECT_EQ(7, message.seq);
    EXPECT_EQ(5, message.requestVote.candidateId);
    EXPECT_EQ(11, message.requestVote.lastLogIndex);
    EXPECT_EQ(3, message.requestVote.lastLogTerm);
}

TEST(MessageTests, DeserializeRequestVoteV1) {
    // Arrange
    const std::vector< char > serializedMessageBytes({
        0x01, // version (1)
        0x01, // type (RequestVote)
        42, // term
        5, // candidate ID
        11, // last log index
        3, // last log term
    });
    const std::string serializedMessage(
        serializedMessageBytes.begin(),
        serializedMessageBytes.end()
    );

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::RequestVote, message.type);
    EXPECT_EQ(42, message.term);
    EXPECT_EQ(0, message.seq);
    EXPECT_EQ(5, message.requestVote.candidateId);
    EXPECT_EQ(11, message.requestVote.lastLogIndex);
    EXPECT_EQ(3, message.requestVote.lastLogTerm);
}

TEST(MessageTests, RequestVoteResults) {
    // Arrange
    Raft::Message messageIn;
    messageIn.type = Raft::Message::Type::RequestVoteResults;
    messageIn.term = 16;
    messageIn.seq = 8;
    messageIn.requestVoteResults.voteGranted = true;
    const auto serializedMessage = messageIn.Serialize();

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::RequestVoteResults, message.type);
    EXPECT_EQ(16, message.term);
    EXPECT_EQ(8, message.seq);
    EXPECT_TRUE(message.requestVoteResults.voteGranted);
}

TEST(MessageTests, HeartBeat) {
    // Arrange
    Raft::Message messageIn;
    messageIn.type = Raft::Message::Type::AppendEntries;
    messageIn.term = 8;
    messageIn.seq = 7;
    messageIn.appendEntries.leaderCommit = 18;
    messageIn.appendEntries.prevLogIndex = 6;
    messageIn.appendEntries.prevLogTerm = 1;
    const auto serializedMessage = messageIn.Serialize();

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::AppendEntries, message.type);
    EXPECT_EQ(8, message.term);
    EXPECT_EQ(7, message.seq);
    EXPECT_EQ(18, message.appendEntries.leaderCommit);
    EXPECT_EQ(6, message.appendEntries.prevLogIndex);
    EXPECT_EQ(1, message.appendEntries.prevLogTerm);
}

TEST(MessageTests, AppendEntriesWithContent) {
    // Arrange
    Raft::Message messageIn;
    messageIn.type = Raft::Message::Type::AppendEntries;
    messageIn.term = 8;
    messageIn.seq = 9;
    messageIn.appendEntries.leaderCommit = 33;
    messageIn.appendEntries.prevLogIndex = 5;
    messageIn.appendEntries.prevLogTerm = 6;
    Raft::LogEntry firstEntry;
    firstEntry.term = 7;
    messageIn.log.push_back(std::move(firstEntry));
    Raft::LogEntry secondEntry;
    secondEntry.term = 8;
    auto command = std::make_shared< Raft::SingleConfigurationCommand >();
    command->oldConfiguration.instanceIds = {2, 5, 6, 7, 11};
    command->configuration.instanceIds = {2, 5, 6, 7, 12};
    secondEntry.command = std::move(command);
    messageIn.log.push_back(std::move(secondEntry));
    const auto serializedMessage = messageIn.Serialize();

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::AppendEntries, message.type);
    EXPECT_EQ(8, message.term);
    EXPECT_EQ(9, message.seq);
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

TEST(MessageTests, DeserializeGarbage) {
    // Arrange
    const auto serializedMessage = "PogChamp";

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::Unknown, message.type);
}

TEST(MessageTests, AppendEntriesResults) {
    // Arrange
    Raft::Message messageIn;
    messageIn.type = Raft::Message::Type::AppendEntriesResults;
    messageIn.term = 5;
    messageIn.seq = 4;
    messageIn.appendEntriesResults.success = false;
    messageIn.appendEntriesResults.matchIndex = 10;
    const auto serializedMessage = messageIn.Serialize();

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, message.type);
    EXPECT_EQ(5, message.term);
    EXPECT_EQ(4, message.seq);
    EXPECT_FALSE(message.appendEntriesResults.success);
    EXPECT_EQ(10, message.appendEntriesResults.matchIndex);
}

TEST(MessageTests, InstallSnapshot) {
    // Arrange
    Raft::Message messageIn;
    messageIn.type = Raft::Message::Type::InstallSnapshot;
    messageIn.term = 8;
    messageIn.seq = 2;
    messageIn.installSnapshot.lastIncludedIndex = 2;
    messageIn.installSnapshot.lastIncludedTerm = 7;
    messageIn.snapshot = Json::Object({
        {"foo", "bar"},
    });
    const auto serializedMessage = messageIn.Serialize();

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::InstallSnapshot, message.type);
    EXPECT_EQ(8, message.term);
    EXPECT_EQ(2, message.seq);
    EXPECT_EQ(2, message.installSnapshot.lastIncludedIndex);
    EXPECT_EQ(7, message.installSnapshot.lastIncludedTerm);
    EXPECT_EQ(messageIn.snapshot, message.snapshot);
}

TEST(MessageTests, InstallSnapshotResults) {
    // Arrange
    Raft::Message messageIn;
    messageIn.type = Raft::Message::Type::InstallSnapshotResults;
    messageIn.term = 8;
    messageIn.seq = 17;
    messageIn.installSnapshotResults.matchIndex = 100;
    const auto serializedMessage = messageIn.Serialize();

    // Act
    Raft::Message message(serializedMessage);

    // Act
    EXPECT_EQ(Raft::Message::Type::InstallSnapshotResults, message.type);
    EXPECT_EQ(8, message.term);
    EXPECT_EQ(17, message.seq);
    EXPECT_EQ(100, message.installSnapshotResults.matchIndex);
}
