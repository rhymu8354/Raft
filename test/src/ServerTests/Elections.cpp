/**
 * @file Elections.cpp
 *
 * This module contains the unit tests of the Raft::Server class that have
 * to do with the election process/aspects of the Raft Consensus algorithm.
 *
 * © 2019-2020 by Richard Walters
 */

#include "Common.hpp"

#include <algorithm>
#include <condition_variable>
#include <functional>
#include <gtest/gtest.h>
#include <Json/Value.hpp>
#include <limits>
#include <mutex>
#include <Raft/ILog.hpp>
#include <Raft/IPersistentState.hpp>
#include <Raft/LogEntry.hpp>
#include <Raft/Server.hpp>
#include <stddef.h>
#include <vector>

namespace ServerTests {

    /**
     * This is the test fixture for these tests, providing common
     * setup and teardown for each test.
     */
    struct ServerTests_Elections
        : public Common
    {
    };

    TEST_F(ServerTests_Elections, Election_Never_Starts_Before_Minimum_Timeout_Interval) {
        // Arrange
        MobilizeServer();

        // Act
        AdvanceTimeToJustBeforeElectionTimeout();

        // Assert
        EXPECT_FALSE(AwaitMessagesSent(1));
    }

    TEST_F(ServerTests_Elections, Election_Always_Started_Within_Maximum_Timeout_Interval) {
        // Arrange
        MobilizeServer();

        // Act
        const auto electionStarted = AwaitElectionTimeout();

        // Assert
        EXPECT_TRUE(electionStarted);
        EXPECT_EQ(
            Raft::IServer::ElectionState::Candidate,
            server.GetElectionState()
        );
    }

    // TODO: This test takes a REALLY long time to run because
    // we don't have an easy quick way to synchronize with the Raft
    // message queueing, and must wait up to 100ms to check if a message
    // is queued or not.  Multiplied by the sheer number of checks that
    // this test performs, it's not practical to run it currently.
    //
    // TEST_F(ServerTests_Elections, Election_Starts_After_Random_Interval_Between_Minimum_And_Maximum) {
    //     // Arrange
    //     MobilizeServer();

    //     // Act
    //     const auto binInterval = 0.001;
    //     std::vector< size_t > bins(
    //         (size_t)(
    //             (serverConfiguration.maximumElectionTimeout - serverConfiguration.minimumElectionTimeout)
    //             / binInterval
    //         )
    //     );
    //     for (size_t i = 0; i < 100; ++i) {
    //         messagesSent.clear();
    //         mockTimeKeeper->currentTime = 0.0;
    //         server.Demobilize();
    //         MobilizeServer();
    //         mockTimeKeeper->currentTime = serverConfiguration.minimumElectionTimeout + binInterval;
    //         for (
    //             size_t j = 0;
    //             mockTimeKeeper->currentTime <= serverConfiguration.maximumElectionTimeout;
    //             ++j, mockTimeKeeper->currentTime += binInterval
    //         ) {
    //             scheduler->WakeUp();
    //             if (AwaitMessagesSent(1)) {
    //                 if (bins.size() <= j) {
    //                     bins.resize(j + 1);
    //                 }
    //                 ++bins[j];
    //                 break;
    //             }
    //         }
    //     }

    //     // Assert
    //     bins.resize(bins.size() - 1);
    //     size_t smallestBin = std::numeric_limits< size_t >::max();
    //     size_t largestBin = std::numeric_limits< size_t >::min();
    //     for (auto bin: bins) {
    //         smallestBin = std::min(smallestBin, bin);
    //         largestBin = std::max(largestBin, bin);
    //     }
    //     EXPECT_GE(largestBin - smallestBin, 5);
    //     EXPECT_LE(largestBin - smallestBin, 20);
    // }

    TEST_F(ServerTests_Elections, Request_Vote_Sent_To_All_Servers_Except_Self) {
        // Arrange
        MobilizeServer();

        // Act
        ASSERT_TRUE(AwaitElectionTimeout());

        // Assert
        std::set< int > instances(
            clusterConfiguration.instanceIds.begin(),
            clusterConfiguration.instanceIds.end()
        );
        for (const auto messageInfo: messagesSent) {
            EXPECT_EQ(
                Raft::Message::Type::RequestVote,
                messageInfo.message.type
            );
            EXPECT_EQ(1, messageInfo.message.seq);
            (void)instances.erase(messageInfo.receiverInstanceNumber);
        }
        EXPECT_EQ(
            std::set< int >{ serverConfiguration.selfInstanceId },
            instances
        );
    }

    TEST_F(ServerTests_Elections, Request_Vote_Includes_Last_Index) {
        // Arrange
        MobilizeServer();
        server.SetLastIndex(42);

        // Act
        ASSERT_TRUE(AwaitElectionTimeout());

        // Assert
        for (const auto messageInfo: messagesSent) {
            EXPECT_EQ(
                42,
                messageInfo.message.requestVote.lastLogIndex
            );
        }
    }

    TEST_F(ServerTests_Elections, Request_Vote_Includes_Last_Term_From_Log) {
        // Arrange
        AppendNoOpEntry(3);
        AppendNoOpEntry(7);
        MobilizeServer();

        // Act
        ASSERT_TRUE(AwaitElectionTimeout());

        // Assert
        for (const auto messageInfo: messagesSent) {
            EXPECT_EQ(
                7,
                messageInfo.message.requestVote.lastLogTerm
            );
        }
    }

    TEST_F(ServerTests_Elections, Request_Vote_Includes_Last_Term_From_Snapshot) {
        // Arrange
        mockLog->baseIndex = 100;
        mockLog->commitIndex = 100;
        mockLog->baseTerm = 42;
        MobilizeServer();

        // Act
        ASSERT_TRUE(AwaitElectionTimeout());

        // Assert
        for (const auto messageInfo: messagesSent) {
            EXPECT_EQ(
                42,
                messageInfo.message.requestVote.lastLogTerm
            );
        }
    }

    TEST_F(ServerTests_Elections, Request_Vote_Includes_Last_Term_Without_Log) {
        // Arrange
        MobilizeServer();

        // Act
        ASSERT_TRUE(AwaitElectionTimeout());

        // Assert
        for (const auto messageInfo: messagesSent) {
            EXPECT_EQ(
                0,
                messageInfo.message.requestVote.lastLogTerm
            );
        }
        EXPECT_FALSE(mockLog->invalidEntryIndexed);
    }

    TEST_F(ServerTests_Elections, Server_Votes_For_Itself_In_Election_It_Starts) {
        // Arrange
        MobilizeServer();

        // Act
        ASSERT_TRUE(AwaitElectionTimeout());

        // Assert
        EXPECT_EQ(4, messagesSent.size());
        for (auto messageInfo: messagesSent) {
            EXPECT_EQ(5, messageInfo.message.requestVote.candidateId);
        }
    }

    TEST_F(ServerTests_Elections, Server_Increments_Term_In_Election_It_Starts) {
        // Arrange
        MobilizeServer();

        // Act
        ASSERT_TRUE(AwaitElectionTimeout());

        // Assert
        EXPECT_EQ(4, messagesSent.size());
        for (const auto messageInfo: messagesSent) {
            EXPECT_EQ(1, messageInfo.message.term);
        }
    }

    TEST_F(ServerTests_Elections, Server_Does_Receive_Unanimous_Vote_In_Election) {
        // Arrange
        MobilizeServer();
        ASSERT_TRUE(AwaitElectionTimeout());

        // Act
        CastVotes(1);

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Leader,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, Server_Does_Receive_Non_Unanimous_Majority_Vote_In_Election) {
        // Arrange
        MobilizeServer();
        ASSERT_TRUE(AwaitElectionTimeout());

        // Act
        for (auto instance: clusterConfiguration.instanceIds) {
            CastVote(instance, 1, instance != 11);
        }

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Leader,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, Server_Retransmits_Request_Vote_For_Slow_Voters_In_Election) {
        // Arrange
        //
        // In this scenario, server 5 is a candidate, and receives
        // one less than the minimum number of votes required to
        // be leader.  One server (2) has not yet cast their vote.
        //
        // Server 5 will retransmit a vote request to server 2, but it should
        // not do so until the retransmission time (rpcTimeout) has elapsed.
        //
        // server IDs:     {2, 5, 6, 7, 11}
        // candidate:          ^
        // voting for:                  ^
        // voting against:        ^  ^
        // didn't vote:     ^
        MobilizeServer();
        ASSERT_TRUE(AwaitElectionTimeout());
        for (auto instance: clusterConfiguration.instanceIds) {
            switch (instance) {
                case 6:
                case 7:
                case 11: {
                    CastVote(instance, 1, instance == 11);
                }
            }
        }

        // Act
        messagesSent.clear();
        mockTimeKeeper->currentTime += serverConfiguration.rpcTimeout;
        scheduler->WakeUp();

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(
            Raft::Message::Type::RequestVote,
            messagesSent[0].message.type
        );
        EXPECT_EQ(
            serverConfiguration.selfInstanceId,
            messagesSent[0].message.requestVote.candidateId
        );
        EXPECT_EQ(
            1,
            messagesSent[0].message.term
        );
    }

    TEST_F(ServerTests_Elections, Server_Does_Not_Receive_Any_Votes_In_Election) {
        // Arrange
        MobilizeServer();
        ASSERT_TRUE(AwaitElectionTimeout());

        // Act
        CastVotes(1, false);

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Candidate,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, Server_Almost_Wins_Election) {
        // Arrange
        MobilizeServer();
        ASSERT_TRUE(AwaitElectionTimeout());

        // Act
        for (auto instance: clusterConfiguration.instanceIds) {
            CastVote(
                instance,
                1,
                !(
                    (instance == 2)
                    || (instance == 6)
                    || (instance == 11)
                )
            );
        }

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Candidate,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, Timeout_Before_Majority_Vote_Or_New_Leader_Heart_Beat) {
        // Arrange
        serverConfiguration.minimumElectionTimeout = 0.1;
        serverConfiguration.maximumElectionTimeout = 0.2;
        serverConfiguration.rpcTimeout = 0.3;
        BecomeCandidate(1);

        // Act
        const auto secondElectionStarted = AwaitElectionTimeout();

        // Assert
        EXPECT_TRUE(secondElectionStarted);
        for (const auto messageInfo: messagesSent) {
            EXPECT_EQ(
                Raft::Message::Type::RequestVote,
                messageInfo.message.type
            );
            EXPECT_EQ(2, messageInfo.message.term);
        }
    }

    TEST_F(ServerTests_Elections, Receive_Vote_Request_When_No_Vote_Pending) {
        // Arrange
        MobilizeServer();

        // Act
        Raft::Message message;
        message.type = Raft::Message::Type::RequestVote;
        message.term = 42;
        message.seq = 7;
        message.requestVote.candidateId = 2;
        message.requestVote.lastLogTerm = 41;
        message.requestVote.lastLogIndex = 0;
        server.ReceiveMessage(message.Serialize(), 2);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(42, messagesSent[0].message.term);
        EXPECT_EQ(7, messagesSent[0].message.seq);
        EXPECT_TRUE(messagesSent[0].message.requestVoteResults.voteGranted);
        EXPECT_EQ(
            std::vector< Json::Value >({
                Json::Object({
                    {"term", 42},
                    {"electionState", "follower"},
                    {"didVote", true},
                    {"votedFor", 2},
                }),
            }),
            electionStateChanges
        );
    }

    TEST_F(ServerTests_Elections, Receive_Vote_Request_When_Our_Log_Is_Greater_Term_No_Snapshot) {
        // Arrange
        mockPersistentState->variables.currentTerm = 2;
        AppendNoOpEntry(2);
        MobilizeServer();

        // Act
        RequestVote(
            2, // instance ID
            3, // term
            1, // index of last log entry
            1  // term of last log entry
        );

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(3, messagesSent[0].message.term);
        EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, Receive_Vote_Request_When_Our_Log_Is_Greater_Term_No_Log_With_Snapshot) {
        // Arrange
        mockPersistentState->variables.currentTerm = 2;
        mockLog->baseIndex = 100;
        mockLog->commitIndex = 100;
        mockLog->baseTerm = 2;
        MobilizeServer();

        // Act
        RequestVote(
            2, // instance ID
            3, // term
            100, // index of last log entry
            1  // term of last log entry
        );

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(3, messagesSent[0].message.term);
        EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, Receive_Vote_Request_When_Our_Log_Is_Same_Term_Greater_Index) {
        // Arrange
        mockPersistentState->variables.currentTerm = 2;
        AppendNoOpEntry(1); // index 1
        AppendNoOpEntry(1); // index 2
        MobilizeServer();

        // Act
        RequestVote(
            2, // instance ID
            3, // term
            1 // index of last log entry
        );

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(3, messagesSent[0].message.term);
        EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, Receive_Vote_Request_When_Same_Term_Already_Voted_For_Another) {
        // Arrange
        MobilizeServer();
        RequestVote(2, 1, 0);
        ASSERT_TRUE(AwaitMessagesSent(1));
        messagesSent.clear();

        // Act
        RequestVote(6, 1, 0);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(1, messagesSent[0].message.term);
        EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, Receive_Vote_Request_When_Same_Term_Already_Voted_For_Same) {
        // Arrange
        MobilizeServer();
        RequestVote(2, 1, 0);
        ASSERT_TRUE(AwaitMessagesSent(1));
        messagesSent.clear();

        // Act
        RequestVote(2, 1, 0);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(1, messagesSent[0].message.term);
        EXPECT_TRUE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, Receive_Vote_Request_Before_Minimum_Election_Timeout_After_Last_Leader_Message) {
        // Arrange
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(2, 2);

        // Act
        AdvanceTimeToJustBeforeElectionTimeout();
        RequestVote(6, 3, 0);

        // Assert
        EXPECT_FALSE(AwaitMessagesSent(1));
    }

    TEST_F(ServerTests_Elections, Deny_Vote_Request_Lesser_Term) {
        // Arrange
        mockPersistentState->variables.currentTerm = 2;
        MobilizeServer();

        // Act
        RequestVote(2, 1, 0);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(2, messagesSent[0].message.term);
        EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, Grant_Vote_Request_Greater_Term_When_Follower) {
        // Arrange
        mockPersistentState->variables.currentTerm = 1;
        MobilizeServer();

        // Act
        RequestVote(2, 2, 0);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(2, messagesSent[0].message.term);
        EXPECT_TRUE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, Do_Not_Vote_But_Update_Term_From_Vote_Request_Greater_Term_When_Non_Voting_Member) {
        // Arrange
        mockPersistentState->variables.currentTerm = 1;
        AppendNoOpEntry(1);
        clusterConfiguration.instanceIds = {5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;
        MobilizeServer();

        // Act
        RequestVote(5, 2, 0);

        // Assert
        EXPECT_FALSE(AwaitMessagesSent(1));
        EXPECT_EQ(2, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests_Elections, Grant_Vote_Request_Greater_Term_When_Candidate) {
        // Arrange
        mockPersistentState->variables.currentTerm = 0;
        MobilizeServer();
        ASSERT_TRUE(AwaitElectionTimeout());
        messagesSent.clear();

        // Act
        RequestVote(2, 2, 0);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(2, messagesSent[0].message.term);
        EXPECT_TRUE(messagesSent[0].message.requestVoteResults.voteGranted);
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, Reject_Vote_Request_Greater_Term_When_Candidate) {
        // Arrange
        AppendNoOpEntry(1); // make our journal last index 1
        BecomeCandidate(2);

        // Act
        RequestVote(
            2,  // instance ID
            3,  // term (new!)
            0   // index (old!!)
        );

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(3, messagesSent[0].message.term);
        EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, Grant_Vote_Request_Greater_Term_Same_Index_When_Leader) {
        // Arrange
        //
        // Log remains empty, and so when the other server requests a vote
        // with last index 0, it should be accepted.
        serverConfiguration.minimumElectionTimeout = 0.1;
        serverConfiguration.maximumElectionTimeout = 0.2;
        serverConfiguration.heartbeatInterval = 0.3;
        BecomeLeader(1);

        // Act
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout;
        scheduler->WakeUp();
        RequestVote(2, 2, 0);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(2, messagesSent[0].message.term);
        EXPECT_TRUE(messagesSent[0].message.requestVoteResults.voteGranted);
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, Reject_Vote_Request_Greater_Term_Old_Index_When_Leader) {
        // Arrange
        //
        // Append one entry to log, so when the other server requests a vote
        // with last index 0, it should be rejected.
        serverConfiguration.minimumElectionTimeout = 0.1;
        serverConfiguration.maximumElectionTimeout = 0.2;
        serverConfiguration.heartbeatInterval = 0.3;
        AppendNoOpEntry(1);
        BecomeLeader(1);

        // Act
        mockTimeKeeper->currentTime += serverConfiguration.maximumElectionTimeout;
        scheduler->WakeUp();
        RequestVote(2, 2, 0);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(2, messagesSent[0].message.term);
        EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, Do_Not_Start_Vote_When_Already_Leader) {
        // Arrange
        BecomeLeader();

        // Arrange
        ASSERT_TRUE(AwaitElectionTimeout());

        // Assert
        for (const auto& messageSent: messagesSent) {
            EXPECT_NE(
                Raft::Message::Type::RequestVote,
                messageSent.message.type
            );
        }
    }

    TEST_F(ServerTests_Elections, After_Revert_To_Follower_Do_Not_Start_New_Election_Before_Minimum_Timeout) {
        // Arrange
        serverConfiguration.minimumElectionTimeout = 0.1;
        serverConfiguration.maximumElectionTimeout = 0.2;
        serverConfiguration.heartbeatInterval = 0.3;
        BecomeLeader();
        mockTimeKeeper->currentTime += serverConfiguration.maximumElectionTimeout;
        scheduler->WakeUp();
        RequestVote(2, 2, 0);
        ASSERT_TRUE(AwaitMessagesSent(1));
        messagesSent.clear();

        // Act
        AdvanceTimeToJustBeforeElectionTimeout();

        // Assert
        EXPECT_FALSE(AwaitMessagesSent(1));
    }

    TEST_F(ServerTests_Elections, Leader_Should_Revert_To_Follower_When_Greater_Term_Heartbeat_Received) {
        // Arrange
        BecomeLeader();

        // Act
        ReceiveAppendEntriesFromMockLeader(2, 2);

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, Candidate_Should_Revert_To_Follower_When_Greater_Term_Heartbeat_Received) {
        // Arrange
        MobilizeServer();
        ASSERT_TRUE(AwaitElectionTimeout());
        messagesSent.clear();

        // Act
        ReceiveAppendEntriesFromMockLeader(2, 2);

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, Candidate_Should_Revert_To_Follower_When_Same_Term_Heartbeat_Received) {
        // Arrange
        MobilizeServer();
        ASSERT_TRUE(AwaitElectionTimeout());
        messagesSent.clear();

        // Act
        ReceiveAppendEntriesFromMockLeader(2, 1);

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, Candidate_Should_Revert_To_Follower_When_Greater_Term_Vote_Grant_Received) {
        // Arrange
        //
        // Raft rules say "If RPC request or response contains term T >
        // currentTerm: set currentTerm = T, convert to follower (§5.1)"
        //
        // We need to do this even if we're a candidate and we receive
        // an accepted vote, if that vote has a newer term in it.
        BecomeCandidate(1);

        // Act
        CastVote(2, 2);

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
        EXPECT_EQ(2, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests_Elections, Candidate_Should_Revert_To_Follower_When_Greater_Term_Vote_Reject_Received) {
        // Arrange
        //
        // Raft rules say "If RPC request or response contains term T >
        // currentTerm: set currentTerm = T, convert to follower (§5.1)"
        //
        // We need to do this even if we're a candidate and we receive
        // a rejected vote, if that vote has a newer term in it.
        BecomeCandidate(1);

        // Act
        CastVote(2, 2, false);

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
        EXPECT_EQ(2, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests_Elections, Repeat_Votes_Should_Not_Count) {
        // Arrange
        //
        // In this scenario, server 5 is a candidate, and receives
        // one less than the minimum number of votes required to
        // be leader.  One server (2) has not yet cast their vote.
        //
        // When server 11 tries to cast a second vote in favor of the
        // candidate, it should not end the election.
        //
        // server IDs:     {2, 5, 6, 7, 11}
        // candidate:          ^
        // voting for:                  ^
        // voting against:        ^  ^
        // didn't vote:     ^
        MobilizeServer();
        ASSERT_TRUE(AwaitElectionTimeout());
        for (auto instance: clusterConfiguration.instanceIds) {
            switch (instance) {
                case 6:
                case 7:
                case 11: {
                    CastVote(instance, 1, instance == 11);
                }
            }
        }

        // Act
        CastVote(11, 1);

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Candidate,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, Candidate_Should_Revert_To_Follower_When_Greater_Term_Request_Vote_Received) {
        // Arrange
        BecomeCandidate();

        // Act
        RequestVote(
            2,  // instance ID
            3,  // term (new!)
            0   // index (same)
        );

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
        EXPECT_EQ(3, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests_Elections, Leadership_Gain_Announcement) {
        // Arrange

        // Act
        mockPersistentState->variables.currentTerm = 0;
        serverConfiguration.selfInstanceId = 5;
        auto wasLeadershipChangeAnnounced = SetUpToAwaitLeadershipChange();
        BecomeLeader();

        // Assert
        ASSERT_TRUE(Await(wasLeadershipChangeAnnounced));
        EXPECT_EQ(5, leadershipChangeDetails.leaderId);
        EXPECT_EQ(1, leadershipChangeDetails.term);
    }

    TEST_F(ServerTests_Elections, No_Leadership_Gain_When_Not_Yet_Leader) {
        // Arrange
        auto wasLeadershipChangeAnnounced = SetUpToAwaitLeadershipChange();
        mockPersistentState->variables.currentTerm = 0;
        serverConfiguration.selfInstanceId = 5;
        MobilizeServer();
        ASSERT_TRUE(AwaitElectionTimeout());
        auto instanceNumbersEntry = clusterConfiguration.instanceIds.begin();
        size_t votesGranted = 0;

        // Act
        do {
            const auto instance = *instanceNumbersEntry++;
            if (instance == serverConfiguration.selfInstanceId) {
                continue;
            }
            CastVote(instance, 1);
            EXPECT_FALSE(Await(wasLeadershipChangeAnnounced)) << votesGranted;
            ++votesGranted;
        } while (votesGranted + 1 < clusterConfiguration.instanceIds.size() / 2);

        // Assert
    }

    TEST_F(ServerTests_Elections, No_Leadership_Gain_When_Already_Leader) {
        // Arrange
        auto wasLeadershipChangeAnnounced = SetUpToAwaitLeadershipChange();
        mockPersistentState->variables.currentTerm = 0;
        serverConfiguration.selfInstanceId = 5;
        MobilizeServer();
        ASSERT_TRUE(AwaitElectionTimeout());

        // Act
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance == serverConfiguration.selfInstanceId) {
                continue;
            }
            const auto wasLeader = (
                server.GetElectionState() == Raft::IServer::ElectionState::Leader
            );
            CastVote(instance, 1);
            if (server.GetElectionState() == Raft::IServer::ElectionState::Leader) {
                if (wasLeader) {
                    EXPECT_FALSE(Await(wasLeadershipChangeAnnounced));
                } else {
                    EXPECT_TRUE(Await(wasLeadershipChangeAnnounced));
                    wasLeadershipChangeAnnounced = SetUpToAwaitLeadershipChange();
                }
            }
        }

        // Assert
    }

    TEST_F(ServerTests_Elections, Announce_Leader_When_A_Follower) {
        // Arrange
        auto wasLeadershipChangeAnnounced = SetUpToAwaitLeadershipChange();
        constexpr int leaderId = 2;
        constexpr int newTerm = 1;
        MobilizeServer();

        // Act
        mockPersistentState->variables.currentTerm = 0;
        serverConfiguration.selfInstanceId = 5;
        ReceiveAppendEntriesFromMockLeader(leaderId, newTerm);

        // Assert
        ASSERT_TRUE(Await(wasLeadershipChangeAnnounced));
        EXPECT_EQ(leaderId, leadershipChangeDetails.leaderId);
        EXPECT_EQ(newTerm, leadershipChangeDetails.term);
    }

    TEST_F(ServerTests_Elections, Ignore_Request_Vote_Results_If_Follower) {
        // Arrange
        mockPersistentState->variables.currentTerm = 1;
        MobilizeServer();

        // Act
        CastVotes(1);

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, Ignore_Stale_Yes_Vote_From_Previous_Term) {
        // Arrange
        BecomeCandidate(2);

        // Act
        for (auto instance: clusterConfiguration.instanceIds) {
            Raft::Message message;
            message.type = Raft::Message::Type::RequestVoteResults;
            switch (instance) {
                case 2: {
                    message.term = 1;
                    message.requestVoteResults.voteGranted = true;
                } break;

                case 5: { // self
                } break;

                case 6: {
                    message.term = 2;
                    message.requestVoteResults.voteGranted = true;
                } break;

                case 7: {
                    message.term = 2;
                    message.requestVoteResults.voteGranted = false;
                } break;

                case 11: {
                    message.term = 2;
                    message.requestVoteResults.voteGranted = false;
                } break;
            }
            server.ReceiveMessage(message.Serialize(), instance);
        }

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Candidate,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, Ignore_Stale_No_Vote_From_Previous_Term) {
        // Arrange
        BecomeCandidate(2);

        // Act
        for (auto instance: clusterConfiguration.instanceIds) {
            Raft::Message message;
            message.type = Raft::Message::Type::RequestVoteResults;
            switch (instance) {
                case 2: {
                    message.term = 1;
                    message.requestVoteResults.voteGranted = false;
                } break;

                case 5: { // self
                } break;

                case 6: {
                    message.term = 2;
                    message.requestVoteResults.voteGranted = true;
                } break;

                case 7: {
                    message.term = 2;
                    message.requestVoteResults.voteGranted = false;
                } break;

                case 11: {
                    message.term = 2;
                    message.requestVoteResults.voteGranted = false;
                } break;
            }
            server.ReceiveMessage(message.Serialize(), instance);
        }
        messagesSent.clear();
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        scheduler->WakeUp();

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Candidate,
            server.GetElectionState()
        );
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::RequestVote, messagesSent[0].message.type);
        EXPECT_EQ(2, messagesSent[0].message.term);
        EXPECT_EQ(5, messagesSent[0].message.requestVote.candidateId);
    }

    TEST_F(ServerTests_Elections, Persistent_State_Saved_When_Vote_Is_Cast_As_Follower) {
        // Arrange
        MobilizeServer();

        // Act
        RequestVote(2, 1, 0);

        // Assert
        EXPECT_EQ(2, mockPersistentState->variables.votedFor);
        EXPECT_TRUE(mockPersistentState->variables.votedThisTerm);
    }

    TEST_F(ServerTests_Elections, Persistent_State_Saved_When_Vote_Is_Cast_As_Candidate) {
        // Arrange

        // Act
        BecomeCandidate(4);

        // Assert
        EXPECT_TRUE(mockPersistentState->variables.votedThisTerm);
        EXPECT_EQ(serverConfiguration.selfInstanceId, mockPersistentState->variables.votedFor);
        EXPECT_EQ(4, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests_Elections, Crashed_Follower_Restarts_And_Repeats_Vote_Results) {
        // Arrange
        MobilizeServer();
        RequestVote(2, 1, 0);
        ASSERT_TRUE(AwaitMessagesSent(1));
        messagesSent.clear();
        server.Demobilize();
        server = Raft::Server();
        SetServerDelegates();
        MobilizeServer();

        // Act
        RequestVote(2, 1, 0);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(1, messagesSent[0].message.term);
        EXPECT_TRUE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, Crashed_Follower_Restarts_And_Rejects_Vote_From_Different_Candidate) {
        // Arrange
        MobilizeServer();
        RequestVote(2, 1, 0);
        (void)AwaitMessagesSent(1);
        messagesSent.clear();
        server.Demobilize();
        server = Raft::Server();
        SetServerDelegates();
        MobilizeServer();

        // Act
        RequestVote(10, 1, 0);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(1, messagesSent[0].message.term);
        EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, Persistent_State_Update_For_New_Term_When_Receiving_Vote_Request_For_New_Candidate) {
        // Arrange
        mockPersistentState->variables.currentTerm = 4;
        MobilizeServer();

        // Act
        RequestVote(2, 5, 0);

        // Assert
        EXPECT_EQ(5, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests_Elections, Persistent_State_Update_For_New_Term_When_Vote_Rejected_By_Newer_Term_Server) {
        // Arrange
        BecomeCandidate(4);

        // Act
        CastVote(2, 5, false);

        // Assert
        EXPECT_EQ(5, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests_Elections, Apply_Config_Voting_Member_Single_Config_On_Startup) {
        // Arrange
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;
        auto command = std::make_shared< Raft::SingleConfigurationCommand >();
        command->configuration.instanceIds = {2, 5, 6, 7};
        Raft::LogEntry entry;
        entry.term = 6;
        entry.command = std::move(command);
        mockLog->entries = {entry};

        // Act
        MobilizeServer();

        // Assert
        EXPECT_TRUE(server.IsVotingMember());
    }

    TEST_F(ServerTests_Elections, Candidate_Needs_Separate_Majorities_To_Win_During_Joint_Concensus) {
        // Arrange
        constexpr int term = 5;
        serverConfiguration.selfInstanceId = 1;
        clusterConfiguration.instanceIds = {1, 2, 3, 4, 5};
        Raft::ClusterConfiguration newConfiguration(clusterConfiguration);
        newConfiguration.instanceIds = {11, 12, 13};
        const std::set< int > newConfigurationNotIncludingSelfInstanceIds = {11, 12, 13};
        const std::set< int > jointConfigurationNotIncludingSelfInstanceIds = {2, 3, 4, 5, 11, 12, 13};
        const std::set< int > idsOfInstancesSuccessfullyMatchingLog = {1, 2, 3, 4, 5, 11};
        auto command = std::make_shared< Raft::JointConfigurationCommand >();
        command->oldConfiguration.instanceIds = clusterConfiguration.instanceIds;
        command->newConfiguration.instanceIds = newConfiguration.instanceIds;
        Raft::LogEntry entry;
        entry.term = term - 1;
        entry.command = std::move(command);
        mockLog->entries = {entry};
        BecomeCandidate(term);

        // Act
        for (auto instanceNumber: jointConfigurationNotIncludingSelfInstanceIds) {
            CastVote(
                instanceNumber,
                term,
                (
                    idsOfInstancesSuccessfullyMatchingLog.find(instanceNumber)
                    != idsOfInstancesSuccessfullyMatchingLog.end()
                )
            );
        }

        // Assert
        EXPECT_NE(
            Raft::IServer::ElectionState::Leader,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, Votes_Should_Be_Requested_For_New_Servers_When_Starting_Election_During_Joint_Configuration) {
        // Arrange
        constexpr int term = 6;
        clusterConfiguration.instanceIds = {2, 5, 6, 7};
        serverConfiguration.selfInstanceId = 2;
        auto command = std::make_shared< Raft::JointConfigurationCommand >();
        command->oldConfiguration.instanceIds = {2, 5, 6, 7};
        command->newConfiguration.instanceIds = {2, 5, 6, 7, 11};
        Raft::LogEntry entry;
        entry.term = 5;
        entry.command = std::move(command);
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(5, term, {std::move(entry)});

        // Act
        ASSERT_TRUE(AwaitElectionTimeout());

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(4));
        bool voteRequestedFromNewServer = false;
        for (const auto messageInfo: messagesSent) {
            if (
                (messageInfo.message.type == Raft::Message::Type::RequestVote)
                && (messageInfo.receiverInstanceNumber == 11)
            ) {
                voteRequestedFromNewServer = true;
                break;
            }
        }
        EXPECT_TRUE(voteRequestedFromNewServer);
    }

    TEST_F(ServerTests_Elections, Vote_After_Election_Should_Not_Prevent_Append_Entries_Retransmission) {
        // Arrange
        serverConfiguration.selfInstanceId = 2;
        MobilizeServer();
        ASSERT_TRUE(AwaitElectionTimeout());

        // Act
        for (auto instance: clusterConfiguration.instanceIds) {
            if (
                (instance != serverConfiguration.selfInstanceId)
                && (instance != 11)
            ) {
                CastVote(instance, 1);
            }
        }
        ASSERT_TRUE(AwaitMessagesSent(4));

        // Act
        CastVote(11, 1, false);
        messagesSent.clear();
        mockTimeKeeper->currentTime += serverConfiguration.rpcTimeout + 0.001;
        scheduler->WakeUp();

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(4));
        bool retransmissionSentToServer11 = false;
        for (const auto messageInfo: messagesSent) {
            if (messageInfo.receiverInstanceNumber == 11) {
                retransmissionSentToServer11 = true;
                break;
            }
        }
        EXPECT_TRUE(retransmissionSentToServer11);
    }

    TEST_F(ServerTests_Elections, Announce_Election_State_Changes) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int selfId = 5;
        constexpr int firstTerm = 1;
        mockPersistentState->variables.currentTerm = 0;
        serverConfiguration.selfInstanceId = selfId;
        MobilizeServer();

        // Act
        ReceiveAppendEntriesFromMockLeader(leaderId, firstTerm);
        ASSERT_TRUE(AwaitElectionTimeout());
        CastVotes(2);

        // Assert
        ASSERT_TRUE(AwaitElectionStateChanges(3));
        EXPECT_EQ(
            std::vector< Json::Value >({
                Json::Object({
                    {"term", firstTerm},
                    {"electionState", "follower"},
                    {"didVote", false},
                    {"votedFor", 0},
                }),
                Json::Object({
                    {"term", firstTerm + 1},
                    {"electionState", "candidate"},
                    {"didVote", true},
                    {"votedFor", selfId},
                }),
                Json::Object({
                    {"term", firstTerm + 1},
                    {"electionState", "leader"},
                    {"didVote", true},
                    {"votedFor", selfId},
                }),
            }),
            electionStateChanges
        );
    }

    TEST_F(ServerTests_Elections, No_Election_State_Changes_On_Heartbeat) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int selfId = 5;
        constexpr int term = 1;
        mockPersistentState->variables.currentTerm = 0;
        serverConfiguration.selfInstanceId = selfId;
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(leaderId, term);
        ASSERT_TRUE(AwaitElectionStateChanges(1));
        electionStateChanges.clear();

        // Act
        ReceiveAppendEntriesFromMockLeader(leaderId, term);

        // Assert
        EXPECT_FALSE(AwaitElectionStateChanges(1));
    }

    TEST_F(ServerTests_Elections, No_Election_Timeout_While_Receiving_Snapshot) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int term = 5;
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(leaderId, term);
        const auto advanceTime = [this]{
            mockTimeKeeper->currentTime += serverConfiguration.maximumElectionTimeout + 0.001;
            scheduler->WakeUp();
        };
        server.SetOnReceiveMessageCallback(advanceTime);

        // Act
        Raft::Message message;
        message.type = Raft::Message::Type::InstallSnapshot;
        message.term = term;
        message.installSnapshot.lastIncludedIndex = 100;
        message.installSnapshot.lastIncludedTerm = 3;
        message.snapshot = Json::Object({
            {"foo", "bar"},
        });
        server.ReceiveMessage(message.Serialize(), leaderId);
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout - 0.001;
        scheduler->WakeUp();

        // Assert
        EXPECT_FALSE(AwaitMessagesSent(2));
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
    }

}
