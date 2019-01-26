/**
 * @file Elections.cpp
 *
 * This module contains the unit tests of the Raft::Server class that have
 * to do with the election process/aspects of the Raft Consensus algorithm.
 *
 * © 2019 by Richard Walters
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
#include <Raft/TimeKeeper.hpp>
#include <stddef.h>
#include <SystemAbstractions/StringExtensions.hpp>
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

    TEST_F(ServerTests_Elections, ElectionNeverStartsBeforeMinimumTimeoutInterval) {
        // Arrange
        MobilizeServer();

        // Act
        AdvanceTimeToJustBeforeElectionTimeout();

        // Assert
        EXPECT_EQ(0, messagesSent.size());
    }

    TEST_F(ServerTests_Elections, ElectionAlwaysStartedWithinMaximumTimeoutInterval) {
        // Arrange
        MobilizeServer();

        // Act
        WaitForElectionTimeout();

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Candidate,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, ElectionStartsAfterRandomIntervalBetweenMinimumAndMaximum) {
        // Arrange
        MobilizeServer();

        // Act
        const auto binInterval = 0.001;
        std::vector< size_t > bins(
            (size_t)(
                (serverConfiguration.maximumElectionTimeout - serverConfiguration.minimumElectionTimeout)
                / binInterval
            )
        );
        for (size_t i = 0; i < 100; ++i) {
            messagesSent.clear();
            mockTimeKeeper->currentTime = 0.0;
            server.Demobilize();
            MobilizeServer();
            mockTimeKeeper->currentTime = serverConfiguration.minimumElectionTimeout + binInterval;
            for (
                size_t j = 0;
                mockTimeKeeper->currentTime <= serverConfiguration.maximumElectionTimeout;
                ++j, mockTimeKeeper->currentTime += binInterval
            ) {
                server.WaitForAtLeastOneWorkerLoop();
                if (!messagesSent.empty()) {
                    if (bins.size() <= j) {
                        bins.resize(j + 1);
                    }
                    ++bins[j];
                    break;
                }
            }
        }

        // Assert
        bins.resize(bins.size() - 1);
        size_t smallestBin = std::numeric_limits< size_t >::max();
        size_t largestBin = std::numeric_limits< size_t >::min();
        for (auto bin: bins) {
            smallestBin = std::min(smallestBin, bin);
            largestBin = std::max(largestBin, bin);
        }
        EXPECT_LE(largestBin - smallestBin, 20);
    }

    TEST_F(ServerTests_Elections, RequestVoteSentToAllServersExceptSelf) {
        // Arrange
        MobilizeServer();

        // Act
        WaitForElectionTimeout();

        // Assert
        EXPECT_EQ(4, messagesSent.size());
        std::set< int > instances(
            clusterConfiguration.instanceIds.begin(),
            clusterConfiguration.instanceIds.end()
        );
        for (const auto messageInfo: messagesSent) {
            EXPECT_EQ(
                Raft::Message::Type::RequestVote,
                messageInfo.message.type
            );
            (void)instances.erase(messageInfo.receiverInstanceNumber);
        }
        EXPECT_EQ(
            std::set< int >{ serverConfiguration.selfInstanceId },
            instances
        );
    }

    TEST_F(ServerTests_Elections, RequestVoteIncludesLastIndex) {
        // Arrange
        MobilizeServer();
        server.SetLastIndex(42);

        // Act
        WaitForElectionTimeout();

        // Assert
        for (const auto messageInfo: messagesSent) {
            EXPECT_EQ(
                42,
                messageInfo.message.requestVote.lastLogIndex
            );
        }
    }

    TEST_F(ServerTests_Elections, RequestVoteIncludesLastTermWithLog) {
        // Arrange
        AppendNoOpEntry(3);
        AppendNoOpEntry(7);
        MobilizeServer();

        // Act
        WaitForElectionTimeout();

        // Assert
        for (const auto messageInfo: messagesSent) {
            EXPECT_EQ(
                7,
                messageInfo.message.requestVote.lastLogTerm
            );
        }
    }

    TEST_F(ServerTests_Elections, RequestVoteIncludesLastTermWithoutLog) {
        // Arrange
        MobilizeServer();

        // Act
        WaitForElectionTimeout();

        // Assert
        for (const auto messageInfo: messagesSent) {
            EXPECT_EQ(
                0,
                messageInfo.message.requestVote.lastLogTerm
            );
        }
        EXPECT_FALSE(mockLog->invalidEntryIndexed);
    }

    TEST_F(ServerTests_Elections, ServerVotesForItselfInElectionItStarts) {
        // Arrange
        MobilizeServer();

        // Act
        WaitForElectionTimeout();

        // Assert
        EXPECT_EQ(4, messagesSent.size());
        for (auto messageInfo: messagesSent) {
            EXPECT_EQ(5, messageInfo.message.requestVote.candidateId);
        }
    }

    TEST_F(ServerTests_Elections, ServerIncrementsTermInElectionItStarts) {
        // Arrange
        MobilizeServer();

        // Act
        WaitForElectionTimeout();

        // Assert
        EXPECT_EQ(4, messagesSent.size());
        for (const auto messageInfo: messagesSent) {
            EXPECT_EQ(1, messageInfo.message.requestVote.term);
        }
    }

    TEST_F(ServerTests_Elections, ServerDoesReceiveUnanimousVoteInElection) {
        // Arrange
        MobilizeServer();
        WaitForElectionTimeout();

        // Act
        CastVotes(1);

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Leader,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, ServerDoesReceiveNonUnanimousMajorityVoteInElection) {
        // Arrange
        MobilizeServer();
        WaitForElectionTimeout();

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

    TEST_F(ServerTests_Elections, ServerRetransmitsRequestVoteForSlowVotersInElection) {
        // Arrange
        MobilizeServer();
        WaitForElectionTimeout();
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
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        ASSERT_GE(messagesSent.size(), 1);
        EXPECT_EQ(1, messagesSent.size());
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
            messagesSent[0].message.requestVote.term
        );
    }

    TEST_F(ServerTests_Elections, ServerDoesNotReceiveAnyVotesInElection) {
        // Arrange
        MobilizeServer();
        WaitForElectionTimeout();

        // Act
        CastVotes(1, false);

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Candidate,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, ServerAlmostWinsElection) {
        // Arrange
        MobilizeServer();
        WaitForElectionTimeout();

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

    TEST_F(ServerTests_Elections, TimeoutBeforeMajorityVoteOrNewLeaderHeartbeat) {
        // Arrange
        MobilizeServer();
        WaitForElectionTimeout();
        CastVotes(1, false);
        messagesSent.clear();

        // Act
        WaitForElectionTimeout();

        // Assert
        for (const auto messageInfo: messagesSent) {
            EXPECT_EQ(
                Raft::Message::Type::RequestVote,
                messageInfo.message.type
            );
            EXPECT_EQ(2, messageInfo.message.requestVote.term);
        }
    }

    TEST_F(ServerTests_Elections, ReceiveVoteRequestWhenSameTermNoVotePending) {
        // Arrange
        MobilizeServer();

        // Act
        RequestVote(2, 0, 0);

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(0, messagesSent[0].message.requestVoteResults.term);
        EXPECT_TRUE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, ReceiveVoteRequestWhenOurLogIsGreaterTerm) {
        // Arrange
        mockPersistentState->variables.currentTerm = 2;
        AppendNoOpEntry(2);
        MobilizeServer();

        // Act
        RequestVote(2, 3, 1, 1);

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(3, messagesSent[0].message.requestVoteResults.term);
        EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, ReceiveVoteRequestWhenOurLogIsSameTermGreaterIndex) {
        // Arrange
        mockPersistentState->variables.currentTerm = 2;
        AppendNoOpEntry(1);
        AppendNoOpEntry(1);
        MobilizeServer();

        // Act
        RequestVote(2, 3, 1);

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(3, messagesSent[0].message.requestVoteResults.term);
        EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, ReceiveVoteRequestWhenSameTermAlreadyVotedForAnother) {
        // Arrange
        MobilizeServer();
        RequestVote(2, 1, 0);
        messagesSent.clear();

        // Act
        RequestVote(6, 1, 0);

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(1, messagesSent[0].message.requestVoteResults.term);
        EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, ReceiveVoteRequestWhenSameTermAlreadyVotedForSame) {
        // Arrange
        MobilizeServer();
        RequestVote(2, 1, 0);
        messagesSent.clear();

        // Act
        RequestVote(2, 1, 0);

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(1, messagesSent[0].message.requestVoteResults.term);
        EXPECT_TRUE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, GrantVoteRequestLesserTerm) {
        // Arrange
        mockPersistentState->variables.currentTerm = 2;
        MobilizeServer();

        // Act
        RequestVote(2, 1, 0);

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(2, messagesSent[0].message.requestVoteResults.term);
        EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, GrantVoteRequestGreaterTermWhenFollower) {
        // Arrange
        mockPersistentState->variables.currentTerm = 1;
        MobilizeServer();

        // Act
        RequestVote(2, 2, 0);

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(2, messagesSent[0].message.requestVoteResults.term);
        EXPECT_TRUE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, DoNotVoteButUpdateTermFromVoteRequestGreaterTermWhenNonVotingMember) {
        // Arrange
        mockPersistentState->variables.currentTerm = 1;
        AppendNoOpEntry(1);
        clusterConfiguration.instanceIds = {5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;
        MobilizeServer();

        // Act
        RequestVote(5, 2, 0);

        // Assert
        EXPECT_TRUE(messagesSent.empty());
        EXPECT_EQ(2, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests_Elections, RejectVoteRequestFromCandidateWithOlderLog) {
        // Arrange
        mockPersistentState->variables.currentTerm = 1;
        AppendNoOpEntry(1);
        MobilizeServer();

        // Act
        RequestVote(2, 2, 0);

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(2, messagesSent[0].message.requestVoteResults.term);
        EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, GrantVoteRequestGreaterTermWhenCandidate) {
        // Arrange
        mockPersistentState->variables.currentTerm = 0;
        MobilizeServer();
        WaitForElectionTimeout();
        messagesSent.clear();

        // Act
        RequestVote(2, 2, 0);
        AdvanceTimeToJustBeforeElectionTimeout();

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(2, messagesSent[0].message.requestVoteResults.term);
        EXPECT_TRUE(messagesSent[0].message.requestVoteResults.voteGranted);
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, RejectVoteRequestGreaterTermWhenCandidate) {
        // Arrange
        AppendNoOpEntry(1);
        BecomeCandidate(2);
        messagesSent.clear();

        // Act
        RequestVote(2, 3, 0);
        AdvanceTimeToJustBeforeElectionTimeout();

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(3, messagesSent[0].message.requestVoteResults.term);
        EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, GrantVoteRequestGreaterTermWhenLeader) {
        // Arrange
        BecomeLeader(1);

        // Act
        RequestVote(2, 2, 0);

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(2, messagesSent[0].message.requestVoteResults.term);
        EXPECT_TRUE(messagesSent[0].message.requestVoteResults.voteGranted);
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, RejectVoteRequestGreaterTermWhenLeader) {
        // Arrange
        AppendNoOpEntry(1);
        BecomeLeader(1);

        // Act
        RequestVote(2, 2, 0);

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(2, messagesSent[0].message.requestVoteResults.term);
        EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, DoNotStartVoteWhenAlreadyLeader) {
        // Arrange
        BecomeLeader();

        // Arrange
        WaitForElectionTimeout();

        // Assert
        for (const auto& messageSent: messagesSent) {
            EXPECT_NE(
                Raft::Message::Type::RequestVote,
                messageSent.message.type
            );
        }
    }

    TEST_F(ServerTests_Elections, AfterRevertToFollowerDoNotStartNewElectionBeforeMinimumTimeout) {
        // Arrange
        BecomeLeader();
        WaitForElectionTimeout();
        messagesSent.clear();
        RequestVote(2, 2, 0);
        messagesSent.clear();

        // Act
        AdvanceTimeToJustBeforeElectionTimeout();

        // Assert
        EXPECT_EQ(0, messagesSent.size());
    }

    TEST_F(ServerTests_Elections, LeaderShouldRevertToFollowerWhenGreaterTermHeartbeatReceived) {
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

    TEST_F(ServerTests_Elections, CandidateShouldRevertToFollowerWhenGreaterTermHeartbeatReceived) {
        // Arrange
        MobilizeServer();
        WaitForElectionTimeout();
        messagesSent.clear();

        // Act
        ReceiveAppendEntriesFromMockLeader(2, 2);
        CastVotes(1);
        AdvanceTimeToJustBeforeElectionTimeout();

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, CandidateShouldRevertToFollowerWhenSameTermHeartbeatReceived) {
        // Arrange
        MobilizeServer();
        WaitForElectionTimeout();
        messagesSent.clear();

        // Act
        ReceiveAppendEntriesFromMockLeader(2, 1);
        CastVotes(1);
        AdvanceTimeToJustBeforeElectionTimeout();

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, CandidateShouldRevertToFollowerWhenGreaterTermVoteGrantReceived) {
        // Arrange
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

    TEST_F(ServerTests_Elections, CandidateShouldRevertToFollowerWhenGreaterTermVoteRejectReceived) {
        // Arrange
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

    TEST_F(ServerTests_Elections, LeaderShouldSendRegularHeartbeats) {
        // Arrange
        serverConfiguration.heartbeatInterval = 0.001;
        BecomeLeader();

        // Act
        mockTimeKeeper->currentTime += 0.0011;
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        std::map< int, size_t > heartbeatsReceivedPerInstance;
        for (auto instanceNumber: clusterConfiguration.instanceIds) {
            heartbeatsReceivedPerInstance[instanceNumber] = 0;
        }
        for (const auto& messageSent: messagesSent) {
            if (messageSent.message.type == Raft::Message::Type::AppendEntries) {
                ++heartbeatsReceivedPerInstance[messageSent.receiverInstanceNumber];
            }
        }
        for (auto instanceNumber: clusterConfiguration.instanceIds) {
            if (instanceNumber == serverConfiguration.selfInstanceId) {
                EXPECT_EQ(0, heartbeatsReceivedPerInstance[instanceNumber]);
            } else {
                EXPECT_EQ(1, heartbeatsReceivedPerInstance[instanceNumber]);
            }
        }
    }

    TEST_F(ServerTests_Elections, RepeatVotesShouldNotCount) {
        // Arrange
        MobilizeServer();
        WaitForElectionTimeout();
        for (auto instance: clusterConfiguration.instanceIds) {
            CastVote(1, instance == 2);
        }

        // Act
        CastVote(2, 1);

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Candidate,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, CandidateShouldRevertToFollowerWhenGreaterTermRequestVoteReceived) {
        // Arrange
        BecomeCandidate();

        // Act
        RequestVote(2, 3, 0);
        messagesSent.clear();
        AdvanceTimeToJustBeforeElectionTimeout();

        // Assert
        EXPECT_EQ(0, messagesSent.size());
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
        EXPECT_EQ(3, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests_Elections, CandidateShouldRevertToFollowerWhenGreaterTermRequestVoteResultsReceived) {
        // Arrange
        BecomeCandidate(1);

        // Act
        CastVote(2, 3, false);

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
        EXPECT_EQ(3, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests_Elections, LeadershipGainAnnouncement) {
        // Arrange

        // Act
        mockPersistentState->variables.currentTerm = 0;
        serverConfiguration.selfInstanceId = 5;
        BecomeLeader();

        // Assert
        ASSERT_TRUE(leadershipChangeAnnounced);
        EXPECT_EQ(5, leadershipChangeDetails.leaderId);
        EXPECT_EQ(1, leadershipChangeDetails.term);
    }

    TEST_F(ServerTests_Elections, NoLeadershipGainWhenNotYetLeader) {
        // Arrange

        // Act
        mockPersistentState->variables.currentTerm = 0;
        serverConfiguration.selfInstanceId = 5;
        MobilizeServer();
        WaitForElectionTimeout();
        auto instanceNumbersEntry = clusterConfiguration.instanceIds.begin();
        size_t votesGranted = 0;
        do {
            const auto instance = *instanceNumbersEntry++;
            if (instance == serverConfiguration.selfInstanceId) {
                continue;
            }
            CastVote(instance, 1);
            EXPECT_FALSE(leadershipChangeAnnounced) << votesGranted;
            ++votesGranted;
        } while (votesGranted + 1 < clusterConfiguration.instanceIds.size() / 2);

        // Assert
    }

    TEST_F(ServerTests_Elections, NoLeadershipGainWhenAlreadyLeader) {
        // Arrange

        // Act
        mockPersistentState->variables.currentTerm = 0;
        serverConfiguration.selfInstanceId = 5;
        MobilizeServer();
        WaitForElectionTimeout();
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
                    EXPECT_FALSE(leadershipChangeAnnounced);
                } else {
                    EXPECT_TRUE(leadershipChangeAnnounced);
                }
            }
            leadershipChangeAnnounced = false;
        }

        // Assert
    }

    TEST_F(ServerTests_Elections, AnnounceLeaderWhenAFollower) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int newTerm = 1;
        MobilizeServer();

        // Act
        mockPersistentState->variables.currentTerm = 0;
        serverConfiguration.selfInstanceId = 5;
        ReceiveAppendEntriesFromMockLeader(leaderId, newTerm);

        // Assert
        ASSERT_TRUE(leadershipChangeAnnounced);
        EXPECT_EQ(leaderId, leadershipChangeDetails.leaderId);
        EXPECT_EQ(newTerm, leadershipChangeDetails.term);
    }

    TEST_F(ServerTests_Elections, IgnoreRequestVoteResultsIfFollower) {
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

    TEST_F(ServerTests_Elections, IgnoreStaleYesVoteFromPreviousTerm) {
        // Arrange
        BecomeCandidate(2);

        // Act
        for (auto instance: clusterConfiguration.instanceIds) {
            Raft::Message message;
            message.type = Raft::Message::Type::RequestVoteResults;
            switch (instance) {
                case 2: {
                    message.requestVoteResults.term = 1;
                    message.requestVoteResults.voteGranted = true;
                } break;

                case 5: { // self
                } break;

                case 6: {
                    message.requestVoteResults.term = 2;
                    message.requestVoteResults.voteGranted = true;
                } break;

                case 7: {
                    message.requestVoteResults.term = 2;
                    message.requestVoteResults.voteGranted = false;
                } break;

                case 11: {
                    message.requestVoteResults.term = 2;
                    message.requestVoteResults.voteGranted = false;
                } break;
            }
            server.ReceiveMessage(message.Serialize(), instance);
        }
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Candidate,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests_Elections, IgnoreStaleNoVoteFromPreviousTerm) {
        // Arrange
        BecomeCandidate(2);

        // Act
        for (auto instance: clusterConfiguration.instanceIds) {
            Raft::Message message;
            message.type = Raft::Message::Type::RequestVoteResults;
            switch (instance) {
                case 2: {
                    message.requestVoteResults.term = 1;
                    message.requestVoteResults.voteGranted = false;
                } break;

                case 5: { // self
                } break;

                case 6: {
                    message.requestVoteResults.term = 2;
                    message.requestVoteResults.voteGranted = true;
                } break;

                case 7: {
                    message.requestVoteResults.term = 2;
                    message.requestVoteResults.voteGranted = false;
                } break;

                case 11: {
                    message.requestVoteResults.term = 2;
                    message.requestVoteResults.voteGranted = false;
                } break;
            }
            server.ReceiveMessage(message.Serialize(), instance);
        }
        messagesSent.clear();
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Candidate,
            server.GetElectionState()
        );
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::RequestVote, messagesSent[0].message.type);
        EXPECT_EQ(2, messagesSent[0].message.requestVote.term);
        EXPECT_EQ(5, messagesSent[0].message.requestVote.candidateId);
    }

    TEST_F(ServerTests_Elections, PersistentStateSavedWhenVoteIsCastAsFollower) {
        // Arrange
        MobilizeServer();

        // Act
        RequestVote(2, 1, 0);

        // Assert
        EXPECT_EQ(2, mockPersistentState->variables.votedFor);
        EXPECT_TRUE(mockPersistentState->variables.votedThisTerm);
    }

    TEST_F(ServerTests_Elections, PersistentStateSavedWhenVoteIsCastAsCandidate) {
        // Arrange

        // Act
        BecomeCandidate(4);

        // Assert
        EXPECT_TRUE(mockPersistentState->variables.votedThisTerm);
        EXPECT_EQ(serverConfiguration.selfInstanceId, mockPersistentState->variables.votedFor);
        EXPECT_EQ(4, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests_Elections, CrashedFollowerRestartsAndRepeatsVoteResults) {
        // Arrange
        MobilizeServer();
        RequestVote(2, 1, 0);
        messagesSent.clear();
        server.Demobilize();
        server = Raft::Server();
        SetServerDelegates();
        MobilizeServer();

        // Act
        RequestVote(2, 1, 0);

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(1, messagesSent[0].message.requestVoteResults.term);
        EXPECT_TRUE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, CrashedFollowerRestartsAndRejectsVoteFromDifferentCandidate) {
        // Arrange
        MobilizeServer();
        RequestVote(2, 1, 0);
        messagesSent.clear();
        server.Demobilize();
        server = Raft::Server();
        SetServerDelegates();
        MobilizeServer();

        // Act
        RequestVote(10, 1, 0);

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(
            Raft::Message::Type::RequestVoteResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(1, messagesSent[0].message.requestVoteResults.term);
        EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
    }

    TEST_F(ServerTests_Elections, PersistentStateUpdateForNewTermWhenReceivingVoteRequestForNewCandidate) {
        // Arrange
        mockPersistentState->variables.currentTerm = 4;
        MobilizeServer();

        // Act
        RequestVote(2, 5, 0);

        // Assert
        EXPECT_EQ(5, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests_Elections, PersistentStateUpdateForNewTermWhenVoteRejectedByNewerTermServer) {
        // Arrange
        BecomeCandidate(4);

        // Act
        CastVote(2, 5, false);

        // Assert
        EXPECT_EQ(5, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests_Elections, ApplyConfigVotingMemberSingleConfigOnStartup) {
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
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        EXPECT_TRUE(server.IsVotingMember());
    }

    TEST_F(ServerTests_Elections, CandidateNeedsSeparateMajoritiesToWinDuringJointConcensus) {
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

    TEST_F(ServerTests_Elections, VotesShouldBeRequestedForNewServersWhenStartingElectionDuringJointConfiguration) {
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
        WaitForElectionTimeout();

        // Assert
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

    TEST_F(ServerTests_Elections, VoteAfterElectionShouldNotPreventAppendEntriesRetransmission) {
        // Arrange
        serverConfiguration.selfInstanceId = 2;
        MobilizeServer();
        WaitForElectionTimeout();

        // Act
        for (auto instance: clusterConfiguration.instanceIds) {
            if (
                (instance != serverConfiguration.selfInstanceId)
                && (instance != 11)
            ) {
                CastVote(instance, 1);
            }
        }
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Act
        CastVote(11, 1, false);
        messagesSent.clear();
        mockTimeKeeper->currentTime += serverConfiguration.rpcTimeout + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        bool retransmissionSentToServer11 = false;
        for (const auto messageInfo: messagesSent) {
            if (messageInfo.receiverInstanceNumber == 11) {
                retransmissionSentToServer11 = true;
                break;
            }
        }
        EXPECT_TRUE(retransmissionSentToServer11);
    }

    TEST_F(ServerTests_Elections, AnnounceElectionStateChanges) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int selfId = 5;
        constexpr int firstTerm = 1;
        mockPersistentState->variables.currentTerm = 0;
        serverConfiguration.selfInstanceId = selfId;
        MobilizeServer();

        // Act
        ReceiveAppendEntriesFromMockLeader(leaderId, firstTerm);
        WaitForElectionTimeout();
        CastVotes(2);

        // Assert
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

    TEST_F(ServerTests_Elections, NoElectionStateChangesOnHeartbeat) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int selfId = 5;
        constexpr int term = 1;
        mockPersistentState->variables.currentTerm = 0;
        serverConfiguration.selfInstanceId = selfId;
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(leaderId, term);
        electionStateChanges.clear();

        // Act
        ReceiveAppendEntriesFromMockLeader(leaderId, term);

        // Assert
        EXPECT_TRUE(electionStateChanges.empty());
    }

}
