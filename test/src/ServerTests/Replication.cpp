/**
 * @file Replication.cpp
 *
 * This module contains the unit tests of the Raft::Server class that have
 * to do with replicating server state across the entire cluster.
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
    struct ServerTests_Replication
        : public Common
    {
    };

    TEST_F(ServerTests_Replication, LeaderAppendLogEntry) {
        // Arrange
        AppendNoOpEntry(1);
        BecomeLeader(3);
        server.SetCommitIndex(1);
        std::vector< Raft::LogEntry > entries;
        Raft::LogEntry firstEntry;
        firstEntry.term = 2;
        entries.push_back(std::move(firstEntry));
        Raft::LogEntry secondEntry;
        secondEntry.term = 3;
        entries.push_back(std::move(secondEntry));

        // Act
        EXPECT_EQ(1, server.GetLastIndex());
        server.AppendLogEntries(entries);
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        EXPECT_EQ(3, server.GetLastIndex());
        ASSERT_EQ(entries.size() + 1, mockLog->entries.size());
        for (size_t i = 1; i < entries.size(); ++i) {
            EXPECT_EQ(entries[i].term, mockLog->entries[i + 1].term);
        }
        std::map< int, bool > appendEntriesReceivedPerInstance;
        for (auto instanceNumber: clusterConfiguration.instanceIds) {
            appendEntriesReceivedPerInstance[instanceNumber] = false;
        }
        for (const auto& messageSent: messagesSent) {
            if (messageSent.message.type == Raft::Message::Type::AppendEntries) {
                appendEntriesReceivedPerInstance[messageSent.receiverInstanceNumber] = true;
                EXPECT_EQ(
                    entries,
                    messageSent.message.log
                );
                EXPECT_EQ(3, messageSent.message.term);
                EXPECT_EQ(1, messageSent.message.appendEntries.leaderCommit);
                EXPECT_EQ(1, messageSent.message.appendEntries.prevLogIndex);
                EXPECT_EQ(1, messageSent.message.appendEntries.prevLogTerm);
            }
        }
        for (auto instanceNumber: clusterConfiguration.instanceIds) {
            if (instanceNumber == serverConfiguration.selfInstanceId) {
                EXPECT_FALSE(appendEntriesReceivedPerInstance[instanceNumber]);
            } else {
                EXPECT_TRUE(appendEntriesReceivedPerInstance[instanceNumber]);
            }
        }
    }

    TEST_F(ServerTests_Replication, LeaderAppendLogEntryFirstAfterSnapshot) {
        // Arrange
        mockLog->baseIndex = 100;
        mockLog->commitIndex = 100;
        mockLog->baseTerm = 2;
        BecomeLeader(3);
        Raft::LogEntry entry;
        entry.term = 3;

        // Act
        server.AppendLogEntries({entry});
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        for (const auto& messageSent: messagesSent) {
            if (messageSent.message.type == Raft::Message::Type::AppendEntries) {
                EXPECT_EQ(3, messageSent.message.term);
                EXPECT_EQ(100, messageSent.message.appendEntries.prevLogIndex);
                EXPECT_EQ(2, messageSent.message.appendEntries.prevLogTerm);
            }
        }
    }

    TEST_F(ServerTests_Replication, FollowerAppendLogEntry) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int newTerm = 9;
        mockPersistentState->variables.currentTerm = 0;
        serverConfiguration.selfInstanceId = 5;
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(leaderId, newTerm);
        std::vector< Raft::LogEntry > entries;
        Raft::LogEntry firstEntry;
        firstEntry.term = 4;
        entries.push_back(std::move(firstEntry));
        Raft::LogEntry secondEntry;
        secondEntry.term = 5;
        entries.push_back(std::move(secondEntry));

        // Act
        EXPECT_EQ(0, server.GetLastIndex());
        ReceiveAppendEntriesFromMockLeader(leaderId, 9, entries);

        // Assert
        EXPECT_EQ(2, server.GetLastIndex());
        ASSERT_EQ(entries.size(), mockLog->entries.size());
        for (size_t i = 0; i < entries.size(); ++i) {
            EXPECT_EQ(entries[i].term, mockLog->entries[i].term);
        }
    }

    TEST_F(ServerTests_Replication, LeaderDoNotAdvanceCommitIndexWhenMajorityOfClusterHasNotYetAppliedLogEntry) {
        // Arrange
        BecomeLeader();
        std::vector< Raft::LogEntry > entries;
        Raft::LogEntry firstEntry;
        firstEntry.term = 1;
        entries.push_back(std::move(firstEntry));
        Raft::LogEntry secondEntry;
        secondEntry.term = 1;
        entries.push_back(std::move(secondEntry));

        // Act
        server.AppendLogEntries(entries);
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        EXPECT_EQ(0, server.GetCommitIndex());
    }

    TEST_F(ServerTests_Replication, LeaderAdvanceCommitIndexWhenMajorityOfClusterHasAppliedLogEntry) {
        // Arrange
        BecomeLeader(7);
        Raft::LogEntry firstEntry;
        firstEntry.term = 6;
        Raft::LogEntry secondEntry;
        secondEntry.term = 7;
        server.AppendLogEntries({firstEntry});
        server.WaitForAtLeastOneWorkerLoop();

        // Act
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance != serverConfiguration.selfInstanceId) {
                ReceiveAppendEntriesResults(instance, 7, 1);
                EXPECT_EQ(0, server.GetCommitIndex());
            }
        }
        server.AppendLogEntries({secondEntry});
        server.WaitForAtLeastOneWorkerLoop();
        size_t successfulResponseCount = 0;
        size_t responseCount = 0;
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance != serverConfiguration.selfInstanceId) {
                if (instance == 2) {
                    ReceiveAppendEntriesResults(instance, 7, 1, false);
                } else {
                    ++successfulResponseCount;
                    ReceiveAppendEntriesResults(instance, 7, 2, true);
                }
                ++responseCount;
                if (successfulResponseCount + 1 > clusterConfiguration.instanceIds.size() - successfulResponseCount - 1) {
                    EXPECT_EQ(2, server.GetCommitIndex()) << successfulResponseCount << " out of " << responseCount;
                    EXPECT_EQ(2, mockLog->commitIndex);
                } else {
                    EXPECT_EQ(0, server.GetCommitIndex()) << successfulResponseCount << " out of " << responseCount;
                    EXPECT_EQ(0, mockLog->commitIndex);
                }
            }
        }

        // Assert
    }

    TEST_F(ServerTests_Replication, FollowerAdvanceCommitIndexWhenMajorityOfClusterHasAppliedLogEntry) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int newTerm = 9;
        mockPersistentState->variables.currentTerm = 0;
        serverConfiguration.selfInstanceId = 5;
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(leaderId, newTerm);

        // Act
        Raft::LogEntry firstEntry;
        firstEntry.term = 4;
        ReceiveAppendEntriesFromMockLeader(leaderId, newTerm, {std::move(firstEntry)});
        EXPECT_EQ(0, server.GetCommitIndex());
        Raft::LogEntry secondEntry;
        secondEntry.term = 5;
        ReceiveAppendEntriesFromMockLeader(leaderId, newTerm, 1, {std::move(secondEntry)});

        // Assert
        EXPECT_EQ(1, server.GetCommitIndex());
        EXPECT_EQ(1, mockLog->commitIndex);
    }

    TEST_F(ServerTests_Replication, AppendEntriesWhenNotLeader) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int newTerm = 1;
        mockPersistentState->variables.currentTerm = 0;
        serverConfiguration.selfInstanceId = 5;
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(leaderId, newTerm);
        std::vector< Raft::LogEntry > entries;
        Raft::LogEntry firstEntry;
        firstEntry.term = 1;
        entries.push_back(std::move(firstEntry));
        Raft::LogEntry secondEntry;
        secondEntry.term = 1;
        entries.push_back(std::move(secondEntry));

        // Act
        server.AppendLogEntries(entries);
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        EXPECT_TRUE(messagesSent.empty());
    }

    TEST_F(ServerTests_Replication, InitializeLastIndex) {
        // Arrange
        AppendNoOpEntry(1);
        AppendNoOpEntry(1);

        // Act
        MobilizeServer();

        // Assert
        EXPECT_EQ(2, server.GetLastIndex());
    }

    TEST_F(ServerTests_Replication, LeaderInitialAppendEntriesFromEndOfLog) {
        // Arrange
        AppendNoOpEntry(3);
        AppendNoOpEntry(7);
        BecomeLeader(8);

        // Act
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        for (const auto& messageSent: messagesSent) {
            if (messageSent.message.type == Raft::Message::Type::AppendEntries) {
                EXPECT_EQ(2, messageSent.message.appendEntries.prevLogIndex);
                EXPECT_EQ(7, messageSent.message.appendEntries.prevLogTerm);
                EXPECT_TRUE(messageSent.message.log.empty());
            }
        }
    }

    TEST_F(ServerTests_Replication, LeaderAppendOlderEntriesAfterDiscoveringFollowerIsBehind) {
        // Arrange
        AppendNoOpEntry(3);
        AppendNoOpEntry(7);
        AppendNoOpEntry(8);
        BecomeLeader(8);
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Act
        messagesSent.clear();
        ReceiveAppendEntriesResults(2, 8, 1, false);

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(Raft::Message::Type::AppendEntries, messagesSent[0].message.type);
        EXPECT_EQ(1, messagesSent[0].message.appendEntries.prevLogIndex);
        EXPECT_EQ(3, messagesSent[0].message.appendEntries.prevLogTerm);
        ASSERT_EQ(2, messagesSent[0].message.log.size());
        EXPECT_EQ(7, messagesSent[0].message.log[0].term);
        EXPECT_EQ(8, messagesSent[0].message.log[1].term);
    }

    TEST_F(ServerTests_Replication, LeaderAppendFirstEntryBasedOnSnapshotAfterDiscoveringFollowerIsBehind) {
        // Arrange
        mockLog->baseIndex = 100;
        mockLog->commitIndex = 100;
        mockLog->baseTerm = 7;
        AppendNoOpEntry(3);
        BecomeLeader(8);
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Act
        messagesSent.clear();
        ReceiveAppendEntriesResults(2, 8, 100, false);

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(Raft::Message::Type::AppendEntries, messagesSent[0].message.type);
        EXPECT_EQ(100, messagesSent[0].message.appendEntries.prevLogIndex);
        EXPECT_EQ(7, messagesSent[0].message.appendEntries.prevLogTerm);
        ASSERT_EQ(1, messagesSent[0].message.log.size());
        EXPECT_EQ(3, messagesSent[0].message.log[0].term);
    }

    TEST_F(ServerTests_Replication, LeaderAppendOnlyLogEntryAfterDiscoveringFollowerHasNoLogAtAllNopeNoSirIAmNewPleaseForgiveMe) {
        // Arrange
        AppendNoOpEntry(3);
        BecomeLeader(8);
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Act
        messagesSent.clear();
        ReceiveAppendEntriesResults(2, 8, 0, false);

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(Raft::Message::Type::AppendEntries, messagesSent[0].message.type);
        EXPECT_EQ(0, messagesSent[0].message.appendEntries.prevLogIndex);
        EXPECT_EQ(0, messagesSent[0].message.appendEntries.prevLogTerm);
        ASSERT_EQ(1, messagesSent[0].message.log.size());
        EXPECT_EQ(3, messagesSent[0].message.log[0].term);
    }

    TEST_F(ServerTests_Replication, AppendEntriesNotSentIfLastNotYetAcknowledged) {
        // Arrange
        Raft::LogEntry testEntry;
        testEntry.term = 2;
        BecomeLeader(8);
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Act
        messagesSent.clear();
        server.AppendLogEntries({testEntry});
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        for (const auto& messageSent: messagesSent) {
            EXPECT_FALSE(
                (messageSent.receiverInstanceNumber == 2)
                && (messageSent.message.type == Raft::Message::Type::AppendEntries)
            );
        }
    }

    TEST_F(ServerTests_Replication, NextIndexAdvancedAndNextEntryAppendedAfterPreviousAcknowledged) {
        // Arrange
        Raft::LogEntry secondEntry;
        secondEntry.term = 3;
        AppendNoOpEntry(2);
        BecomeLeader(8);
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        server.WaitForAtLeastOneWorkerLoop();
        ReceiveAppendEntriesResults(2, 8, 0, false);

        // Act
        messagesSent.clear();
        server.AppendLogEntries({secondEntry});
        server.WaitForAtLeastOneWorkerLoop();
        ReceiveAppendEntriesResults(2, 8, 1, true);

        // Assert
        bool sendEntrySent = false;
        for (const auto& messageSent: messagesSent) {
            if (
                (messageSent.receiverInstanceNumber == 2)
                && (messageSent.message.type == Raft::Message::Type::AppendEntries)
            ) {
                EXPECT_FALSE(sendEntrySent);
                sendEntrySent = true;
                EXPECT_EQ(1, messageSent.message.appendEntries.prevLogIndex);
                EXPECT_EQ(2, messageSent.message.appendEntries.prevLogTerm);
                ASSERT_EQ(1, messageSent.message.log.size());
                EXPECT_EQ(secondEntry.term, messageSent.message.log[0].term);
            }
        }
        EXPECT_TRUE(sendEntrySent);
    }

    TEST_F(ServerTests_Replication, FollowerMatchIndexBeyondWhatLeaderHas) {
        // Arrange
        AppendNoOpEntry(2);
        BecomeLeader(8);
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Act
        messagesSent.clear();
        ReceiveAppendEntriesResults(2, 8, 2, false);

        // Assert
        EXPECT_EQ(1, server.GetMatchIndex(2));
    }

    TEST_F(ServerTests_Replication, NoHeartBeatShouldBeSentWhilePreviousAppendEntriesUnacknowledged) {
        // Arrange
        BecomeLeader();
        messagesSent.clear();
        Raft::LogEntry testEntry;
        testEntry.term = 2;
        server.AppendLogEntries({testEntry});

        // Act
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        for (const auto& messageSent: messagesSent) {
            if (messageSent.message.type == Raft::Message::Type::AppendEntries) {
                EXPECT_FALSE(messageSent.message.log.empty());
            }
        }
    }

    TEST_F(ServerTests_Replication, IgnoreAppendEntriesResultsIfNotLeader) {
        // Arrange
        mockPersistentState->variables.currentTerm = 1;
        MobilizeServer();

        // Act
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance != serverConfiguration.selfInstanceId) {
                ReceiveAppendEntriesResults(instance, 1, 42);
            }
        }
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        for (const auto& messageSent: messagesSent) {
            EXPECT_NE(Raft::Message::Type::AppendEntries, messageSent.message.type);
        }
        EXPECT_EQ(0, server.GetCommitIndex());
    }

    TEST_F(ServerTests_Replication, RetransmitUnacknowledgedAppendEntries) {
        // Arrange
        Raft::LogEntry testEntry;
        testEntry.term = 3;
        BecomeLeader(3);

        // Act
        server.AppendLogEntries({testEntry});
        server.WaitForAtLeastOneWorkerLoop();
        messagesSent.clear();
        mockTimeKeeper->currentTime += serverConfiguration.rpcTimeout + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        std::map< int, bool > appendEntriesReceivedPerInstance;
        for (auto instanceNumber: clusterConfiguration.instanceIds) {
            appendEntriesReceivedPerInstance[instanceNumber] = false;
        }
        for (const auto& messageSent: messagesSent) {
            if (messageSent.message.type == Raft::Message::Type::AppendEntries) {
                EXPECT_EQ(3, messageSent.message.seq);
                appendEntriesReceivedPerInstance[messageSent.receiverInstanceNumber] = true;
                ASSERT_EQ(1, messageSent.message.log.size());
                EXPECT_EQ(3, messageSent.message.log[0].term);
            }
        }
        for (auto instanceNumber: clusterConfiguration.instanceIds) {
            if (instanceNumber == serverConfiguration.selfInstanceId) {
                EXPECT_FALSE(appendEntriesReceivedPerInstance[instanceNumber]);
            } else {
                EXPECT_TRUE(appendEntriesReceivedPerInstance[instanceNumber]);
            }
        }
    }

    TEST_F(ServerTests_Replication, RetransmitUnacknowledgedInstallSnapshot) {
        // Arrange
        mockLog->baseIndex = 100;
        mockLog->commitIndex = 100;
        mockLog->baseTerm = 7;
        mockLog->snapshot = Json::Object({
            {"foo", "bar"}
        });
        BecomeLeader(8, false);
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance == 2) {
                ReceiveAppendEntriesResults(2, 8, 0, false);
            } else if (instance != serverConfiguration.selfInstanceId) {
                ReceiveAppendEntriesResults(instance, 8, mockLog->baseIndex + mockLog->entries.size());
            }
        }
        messagesSent.clear();

        // Act
        mockTimeKeeper->currentTime += serverConfiguration.rpcTimeout + 0.001;
        server.WaitForAtLeastOneWorkerLoop();
        bool installSnapshotRetransmitSentAtRpcTimeout = false;
        for (const auto& messageSent: messagesSent) {
            if (
                (messageSent.message.type == Raft::Message::Type::InstallSnapshot)
            ) {
                installSnapshotRetransmitSentAtRpcTimeout = true;
            }
        }
        messagesSent.clear();
        mockTimeKeeper->currentTime += serverConfiguration.installSnapshotTimeout + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        bool installSnapshotRetransmitSentAtInstallSnapshotTimeout = false;
        for (const auto& messageSent: messagesSent) {
            if (
                (messageSent.message.type == Raft::Message::Type::InstallSnapshot)
            ) {
                installSnapshotRetransmitSentAtInstallSnapshotTimeout = true;
            }
        }
        EXPECT_FALSE(installSnapshotRetransmitSentAtRpcTimeout);
        EXPECT_TRUE(installSnapshotRetransmitSentAtInstallSnapshotTimeout);
    }

    TEST_F(ServerTests_Replication, IgnoreDuplicateAppendEntriesResults) {
        // Arrange
        AppendNoOpEntry(3);
        AppendNoOpEntry(7);
        AppendNoOpEntry(8);
        BecomeLeader(8);
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Act
        ReceiveAppendEntriesResults(2, 8, 1, false, 1);
        messagesSent.clear();
        ReceiveAppendEntriesResults(2, 8, 1, false, 1);

        // Assert
        ASSERT_TRUE(messagesSent.empty());
    }

    TEST_F(ServerTests_Replication, ReinitializeVolatileLeaderStateAfterElection) {
        // Arrange
        BecomeLeader(7);
        Raft::LogEntry testEntry;
        testEntry.term = 7;
        server.AppendLogEntries({testEntry});
        server.WaitForAtLeastOneWorkerLoop();
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance != serverConfiguration.selfInstanceId) {
                ReceiveAppendEntriesResults(instance, 7, 1);
            }
        }
        server.WaitForAtLeastOneWorkerLoop();

        // Act
        ReceiveAppendEntriesFromMockLeader(2, 8, 1);
        WaitForElectionTimeout();
        CastVotes(9);
        messagesSent.clear();

        // Assert
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance != serverConfiguration.selfInstanceId) {
                EXPECT_EQ(2, server.GetNextIndex(instance));
                EXPECT_EQ(0, server.GetMatchIndex(instance));
            }
        }
    }

    TEST_F(ServerTests_Replication, FollowerReceiveAppendEntriesSuccess) {
        // Arrange
        Raft::LogEntry oldConflictingEntry, newConflictingEntry, nextEntry;
        oldConflictingEntry.term = 6;
        newConflictingEntry.term = 7;
        nextEntry.term = 8;
        mockLog->entries = {oldConflictingEntry};
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(2, 8);

        // Act
        messagesSent.clear();
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntries;
        message.term = 8;
        message.seq = 42;
        message.appendEntries.leaderCommit = 1;
        message.appendEntries.prevLogIndex = 0;
        message.appendEntries.prevLogTerm = mockLog->baseTerm;
        message.log = {newConflictingEntry, nextEntry};
        server.ReceiveMessage(message.Serialize(), 2);
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        ASSERT_EQ(2, mockLog->entries.size());
        EXPECT_EQ(newConflictingEntry.term, mockLog->entries[0].term);
        EXPECT_EQ(nextEntry.term, mockLog->entries[1].term);
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_EQ(8, messagesSent[0].message.term);
        EXPECT_EQ(42, messagesSent[0].message.seq);
        EXPECT_TRUE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(2, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, FollowerReceiveAppendEntriesAllOld) {
        // Arrange
        AppendNoOpEntry(6);
        AppendNoOpEntry(7);
        Raft::LogEntry oldEntry;
        oldEntry.term = 6;
        MobilizeServer();

        // Act
        ReceiveAppendEntriesFromMockLeader(2, 8, 0, 0, {oldEntry}, false);

        // Assert
        ASSERT_EQ(2, mockLog->entries.size());
        EXPECT_EQ(6, mockLog->entries[0].term);
        EXPECT_EQ(7, mockLog->entries[1].term);
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_TRUE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(1, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, FollowerReceiveAppendEntriesFailureOldTerm) {
        // Arrange
        Raft::LogEntry oldConflictingEntry, nextEntry;
        oldConflictingEntry.term = 6;
        nextEntry.term = 8;
        mockLog->entries = {oldConflictingEntry};
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(2, 8);

        // Act
        messagesSent.clear();
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntries;
        message.term = 8;
        message.appendEntries.leaderCommit = 1;
        message.appendEntries.prevLogIndex = 1;
        message.appendEntries.prevLogTerm = 7;
        message.log = {nextEntry};
        server.ReceiveMessage(message.Serialize(), 2);
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        ASSERT_EQ(1, mockLog->entries.size());
        EXPECT_EQ(oldConflictingEntry.term, mockLog->entries[0].term);
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_EQ(8, messagesSent[0].message.term);
        EXPECT_FALSE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(0, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, FollowerReceiveAppendEntriesFailurePreviousNotFoundNoEntries) {
        // Arrange
        Raft::LogEntry nextEntry;
        nextEntry.term = 8;
        mockLog->entries = {};
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(2, 8);

        // Act
        messagesSent.clear();
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntries;
        message.term = 8;
        message.appendEntries.leaderCommit = 1;
        message.appendEntries.prevLogIndex = 1;
        message.appendEntries.prevLogTerm = 7;
        message.log = {nextEntry};
        server.ReceiveMessage(message.Serialize(), 2);
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        ASSERT_EQ(0, mockLog->entries.size());
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_EQ(8, messagesSent[0].message.term);
        EXPECT_FALSE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(0, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, FollowerReceiveAppendEntriesFailurePreviousNotFoundButHaveOlderEntry) {
        // Arrange
        AppendNoOpEntry(7);
        MobilizeServer();

        // Act
        messagesSent.clear();
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntries;
        message.term = 8;
        message.appendEntries.leaderCommit = 0;
        message.appendEntries.prevLogIndex = 2;
        message.appendEntries.prevLogTerm = 8;
        server.ReceiveMessage(message.Serialize(), 2);
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        ASSERT_EQ(1, mockLog->entries.size());
        EXPECT_EQ(7, mockLog->entries[0].term);
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_FALSE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(1, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, FollowerReceiveAppendEntriesFailureMismatchingTermPreviousIndex) {
        // Arrange
        AppendNoOpEntry(7);
        AppendNoOpEntry(7);
        MobilizeServer();

        // Act
        messagesSent.clear();
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntries;
        message.term = 8;
        message.appendEntries.leaderCommit = 0;
        message.appendEntries.prevLogIndex = 2;
        message.appendEntries.prevLogTerm = 8;
        server.ReceiveMessage(message.Serialize(), 2);
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        ASSERT_EQ(2, mockLog->entries.size());
        EXPECT_EQ(7, mockLog->entries[0].term);
        EXPECT_EQ(7, mockLog->entries[0].term);
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_FALSE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(1, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, PersistentStateUpdateForNewTermWhenReceiveAppendEntriesFromNewerTermLeader) {
        // Arrange
        mockPersistentState->variables.currentTerm = 4;
        MobilizeServer();

        // Act
        ReceiveAppendEntriesFromMockLeader(2, 5, 95);

        // Assert
        EXPECT_EQ(5, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests_Replication, JointConcensusIsNotAchievedSolelyFromSimpleMajority) {
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
        entry.term = term;
        entry.command = std::move(command);
        mockLog->entries = {entry};
        BecomeLeader(term);

        // Act
        for (auto instanceNumber: jointConfigurationNotIncludingSelfInstanceIds) {
            if (idsOfInstancesSuccessfullyMatchingLog.find(instanceNumber) == idsOfInstancesSuccessfullyMatchingLog.end()) {
                ReceiveAppendEntriesResults(instanceNumber, term, 0, false);
            } else {
                ReceiveAppendEntriesResults(instanceNumber, term, 1, true);
            }
        }

        // Assert
        EXPECT_EQ(0, server.GetCommitIndex());
    }

    TEST_F(ServerTests_Replication, StaleAppendEntriesDeservesAFailureResponse) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int term = 2;
        mockPersistentState->variables.currentTerm = term;
        Raft::LogEntry entry;
        entry.term = term - 1;
        mockLog->entries = {entry};
        MobilizeServer();

        // Act
        ReceiveAppendEntriesFromMockLeader(leaderId, term - 1, 1, 0, {}, false);

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(
            leaderId,
            messagesSent[0].receiverInstanceNumber
        );
        EXPECT_EQ(
            Raft::Message::Type::AppendEntriesResults,
            messagesSent[0].message.type
        );
        EXPECT_EQ(
            term,
            messagesSent[0].message.term
        );
        EXPECT_FALSE(messagesSent[0].message.appendEntriesResults.success);
    }

    TEST_F(ServerTests_Replication, ReceivingHeartBeatsDoesNotCausePersistentStateSaves) {
        // Arrange
        MobilizeServer();
        const auto numSavesAtStart = mockPersistentState->saveCount;

        // Act
        while (mockTimeKeeper->currentTime <= serverConfiguration.maximumElectionTimeout * 2) {
            ReceiveAppendEntriesFromMockLeader(2, 2);
            mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2;
            server.WaitForAtLeastOneWorkerLoop();
        }

        // Assert
        EXPECT_EQ(numSavesAtStart + 1, mockPersistentState->saveCount);
    }

    TEST_F(ServerTests_Replication, IgnoreAppendEntriesSameTermIfLeader) {
        // Arrange
        constexpr int term = 5;
        BecomeLeader(term);

        // Act
        ReceiveAppendEntriesFromMockLeader(2, term);

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Leader,
            server.GetElectionState()
        );
        EXPECT_TRUE(messagesSent.empty());
    }

    TEST_F(ServerTests_Replication, DoNotTellLogKeeperToCommitIfCommitIndexUnchanged) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int term = 5;
        mockPersistentState->variables.currentTerm = term;
        Raft::LogEntry entry;
        entry.term = term;
        mockLog->entries = {entry};
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(leaderId, term, 1, 0, {});
        const auto commitCountStart = mockLog->commitCount;

        // Act
        ReceiveAppendEntriesFromMockLeader(leaderId, term, 1, 0, {});

        // Assert
        EXPECT_EQ(commitCountStart, mockLog->commitCount);
    }

    TEST_F(ServerTests_Replication, DoNotCommitToLogAnyEntriesWeDoNotHave) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int term = 5;
        mockPersistentState->variables.currentTerm = term;
        MobilizeServer();

        // Act
        ReceiveAppendEntriesFromMockLeader(leaderId, term, 1, 0, {});

        // Assert
        EXPECT_EQ(0, mockLog->commitIndex);
    }

    TEST_F(ServerTests_Replication, CommitLogAnyEntriesWeHaveWhenIndexLessThanBasePlusSize) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int term = 5;
        mockPersistentState->variables.currentTerm = term;
        Raft::LogEntry entry;
        entry.term = term;
        mockLog->entries = {entry};
        mockLog->baseIndex = 100;
        MobilizeServer();

        // Act
        ReceiveAppendEntriesFromMockLeader(leaderId, term, 101, 101, {});

        // Assert
        EXPECT_EQ(101, mockLog->commitIndex);
    }

    TEST_F(ServerTests_Replication, StaleServerShouldRevertToFollowerWhenAppendEntryResultsHigherTermReceived) {
        // Arrange
        constexpr int term = 7;
        BecomeLeader(term);
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Act
        ReceiveAppendEntriesResults(2, term + 1, 0, false);

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
        EXPECT_EQ(term + 1, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests_Replication, DoNotRetransmitRequestsToServersNoLongerInTheCluster) {
        // Arrange
        constexpr int term = 5;
        serverConfiguration.selfInstanceId = 2;
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        Raft::ClusterConfiguration newConfiguration(clusterConfiguration);
        newConfiguration.instanceIds = {2, 6, 7, 12};
        auto command = std::make_shared< Raft::SingleConfigurationCommand >();
        command->oldConfiguration.instanceIds = clusterConfiguration.instanceIds;
        command->configuration.instanceIds = newConfiguration.instanceIds;
        Raft::LogEntry entry;
        entry.term = term;
        entry.command = std::move(command);
        BecomeLeader(term);
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Act
        server.AppendLogEntries({entry});
        messagesSent.clear();
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        for (const auto messageInfo: messagesSent) {
            EXPECT_NE(5, messageInfo.receiverInstanceNumber);
        }
    }

    TEST_F(ServerTests_Replication, NewLeaderShouldSendHeartBeatsImmediately) {
        // Arrange

        // Act
        BecomeLeader(1, false);

        // Assert
        std::map< int, bool > heartbeatReceivedPerInstance;
        for (auto instanceNumber: clusterConfiguration.instanceIds) {
            heartbeatReceivedPerInstance[instanceNumber] = false;
        }
        for (const auto& messageSent: messagesSent) {
            if (messageSent.message.type == Raft::Message::Type::AppendEntries) {
                heartbeatReceivedPerInstance[messageSent.receiverInstanceNumber] = true;
            }
        }
        for (auto instanceNumber: clusterConfiguration.instanceIds) {
            if (instanceNumber == serverConfiguration.selfInstanceId) {
                EXPECT_FALSE(heartbeatReceivedPerInstance[instanceNumber]);
            } else {
                EXPECT_TRUE(heartbeatReceivedPerInstance[instanceNumber]);
            }
        }
    }

    TEST_F(ServerTests_Replication, RemobilizeShouldResetLastIndexCache) {
        // Arrange
        constexpr int term = 6;
        clusterConfiguration.instanceIds = {2, 5, 6, 7};
        serverConfiguration.selfInstanceId = 2;
        auto command = std::make_shared< Raft::JointConfigurationCommand >();
        command->oldConfiguration.instanceIds = {2, 5, 6, 7, 11};
        command->newConfiguration.instanceIds = {2, 5, 6, 7, 11, 12};
        Raft::LogEntry entry;
        entry.term = 5;
        entry.command = std::move(command);
        mockLog->entries = {entry};
        MobilizeServer();
        server.WaitForAtLeastOneWorkerLoop();
        server.Demobilize();
        mockLog = std::make_shared< MockLog >();

        // Act
        mockPersistentState->variables.currentTerm = term - 1;
        MobilizeServer();

        // Assert
        EXPECT_FALSE(mockLog->invalidEntryIndexed);
    }

    TEST_F(ServerTests_Replication, MeasureBroadcastTime) {
        // Arrange
        BecomeLeader(1);
        server.ResetStatistics();
        mockTimeKeeper->currentTime += serverConfiguration.heartbeatInterval + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Act
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance != serverConfiguration.selfInstanceId) {
                mockTimeKeeper->currentTime += 0.001;
                ReceiveAppendEntriesResults(instance, 1, 1);
            }
        }

        // Assert
        const auto stats = server.GetStatistics();
        EXPECT_NEAR(0.001, (double)stats["minBroadcastTime"], 0.0001);
        EXPECT_NEAR(0.0025, (double)stats["avgBroadcastTime"], 0.0001);
        EXPECT_NEAR(0.004, (double)stats["maxBroadcastTime"], 0.0001);
    }

    TEST_F(ServerTests_Replication, LeaderDeclareCaughtUpOnceCommitIndexReachesInitialLeaderLastIndex) {
        // Arrange
        constexpr int term = 5;
        Raft::LogEntry entry;
        entry.term = term;
        mockLog->entries = {entry};
        BecomeLeader(term, false);

        // Act
        EXPECT_FALSE(caughtUp);
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance != serverConfiguration.selfInstanceId) {
                if (instance != serverConfiguration.selfInstanceId) {
                    ReceiveAppendEntriesResults(instance, term, 1);
                }
            }
        }
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        EXPECT_TRUE(caughtUp);
    }

    TEST_F(ServerTests_Replication, LeaderMustNotDeclareCaughtUpIfCommitIndexHasNotYetReachedInitialLeaderLastIndexWhenBaseIndexNonZero) {
        // Arrange
        constexpr int term = 5;
        Raft::LogEntry entry1, entry2;
        entry1.term = term;
        entry2.term = term;
        mockLog->entries = {entry1, entry2};
        mockLog->baseIndex = 100;
        BecomeLeader(term, false);

        // Act
        EXPECT_FALSE(caughtUp);
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance != serverConfiguration.selfInstanceId) {
                if (instance != serverConfiguration.selfInstanceId) {
                    ReceiveAppendEntriesResults(instance, term, 101);
                }
            }
        }
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        EXPECT_FALSE(caughtUp);
    }

    TEST_F(ServerTests_Replication, FollowerDeclareCaughtUpOnceCommitIndexReachesInitialLeaderLastIndex) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int newTerm = 9;
        mockPersistentState->variables.currentTerm = 0;
        serverConfiguration.selfInstanceId = 5;
        MobilizeServer();
        Raft::LogEntry entry;
        entry.term = newTerm;
        ReceiveAppendEntriesFromMockLeader(leaderId, newTerm, {entry});

        // Act
        EXPECT_FALSE(caughtUp);
        ReceiveAppendEntriesFromMockLeader(leaderId, newTerm, 1);
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        EXPECT_TRUE(caughtUp);
    }

    TEST_F(ServerTests_Replication, CommitIndexInitializedFromLog) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int term = 5;
        mockPersistentState->variables.currentTerm = term;

        // Act
        mockLog->baseIndex = 123;
        MobilizeServer();

        // Assert
        EXPECT_EQ(123, server.GetCommitIndex());
    }

    TEST_F(ServerTests_Replication, LastIndexInitializedFromLog) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int term = 5;
        mockPersistentState->variables.currentTerm = term;

        // Act
        mockLog->baseIndex = 123;
        MobilizeServer();

        // Assert
        EXPECT_EQ(123, server.GetLastIndex());
    }

    TEST_F(ServerTests_Replication, CorrectMatchIndexInAppendEntriesResultsBasedOnSnapshot) {
        // Arrange
        Raft::LogEntry oldEntry, newEntry;
        oldEntry.term = 7;
        newEntry.term = 8;
        mockLog->entries = {oldEntry};
        mockLog->baseIndex = 100;
        mockLog->commitIndex = 100;
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(2, 8);

        // Act
        ReceiveAppendEntriesFromMockLeader(2, 8, 101, 101, {newEntry}, false);

        // Assert
        ASSERT_EQ(2, mockLog->entries.size());
        EXPECT_EQ(oldEntry.term, mockLog->entries[0].term);
        EXPECT_EQ(newEntry.term, mockLog->entries[1].term);
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_EQ(8, messagesSent[0].message.term);
        EXPECT_TRUE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(102, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, DoNotRewindCommitIndexIfLeaderCommitIsBehind) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int term = 5;
        mockPersistentState->variables.currentTerm = term;
        Raft::LogEntry entry;
        entry.term = term;
        mockLog->entries = {entry};
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(leaderId, term, 1, 0, {});
        const auto commitCountStart = mockLog->commitCount;

        // Act
        ReceiveAppendEntriesFromMockLeader(leaderId, term, 0, 0, {});

        // Assert
        EXPECT_EQ(1, mockLog->commitIndex);
    }

    TEST_F(ServerTests_Replication, FollowerReceiveAppendEntriesWhenLogEmptyBasedOnSnapshot) {
        // Arrange
        Raft::LogEntry newEntry;
        newEntry.term = 8;
        mockLog->baseIndex = 100;
        mockLog->baseTerm = 7;
        mockLog->commitIndex = 100;
        MobilizeServer();

        // Act
        messagesSent.clear();
        ReceiveAppendEntriesFromMockLeader(2, 8, 100, 100, {newEntry}, false);

        // Assert
        ASSERT_EQ(1, mockLog->entries.size());
        EXPECT_EQ(newEntry.term, mockLog->entries[0].term);
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_EQ(0, messagesSent[0].message.term);
        EXPECT_TRUE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(101, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, FollowerReceiveAppendEntriesFailWhenLogBasedOnSnapshot) {
        // Arrange
        Raft::LogEntry newEntry;
        newEntry.term = 8;
        mockLog->baseIndex = 100;
        mockLog->baseTerm = 7;
        mockLog->commitIndex = 100;
        MobilizeServer();

        // Act
        messagesSent.clear();
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntries;
        message.term = 8;
        message.appendEntries.leaderCommit = 100;
        message.appendEntries.prevLogIndex = 101;
        message.appendEntries.prevLogTerm = 8;
        message.log = {newEntry};
        server.ReceiveMessage(message.Serialize(), 2);
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        EXPECT_EQ(0, mockLog->entries.size());
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_EQ(0, messagesSent[0].message.term);
        EXPECT_FALSE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(100, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, LeaderInstallSnapshot) {
        // Arrange
        mockLog->baseIndex = 100;
        mockLog->commitIndex = 100;
        mockLog->baseTerm = 7;
        mockLog->snapshot = Json::Object({
            {"foo", "bar"}
        });
        BecomeLeader(8, false);

        // Act
        messagesSent.clear();
        ReceiveAppendEntriesResults(2, 8, 0, false);

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(Raft::Message::Type::InstallSnapshot, messagesSent[0].message.type);
        EXPECT_EQ(100, messagesSent[0].message.installSnapshot.lastIncludedIndex);
        EXPECT_EQ(7, messagesSent[0].message.installSnapshot.lastIncludedTerm);
        EXPECT_EQ(mockLog->snapshot, messagesSent[0].message.snapshot);
    }

    TEST_F(ServerTests_Replication, LeaderHeartbeatAfterSnapshotInstallation) {
        // Arrange
        mockLog->baseIndex = 100;
        mockLog->commitIndex = 100;
        mockLog->baseTerm = 7;
        mockLog->snapshot = Json::Object({
            {"foo", "bar"}
        });
        BecomeLeader(8, false);
        ReceiveAppendEntriesResults(2, 8, 0, false);

        // Act
        messagesSent.clear();
        Raft::Message message;
        message.type = Raft::Message::Type::InstallSnapshotResults;
        message.term = 8;
        message.installSnapshotResults.matchIndex = 100;
        server.ReceiveMessage(message.Serialize(), 2);
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        bool expectedMessageFound = false;
        for (const auto& messageSent: messagesSent) {
            if (
                (messageSent.message.type == Raft::Message::Type::AppendEntries)
                && (messageSent.receiverInstanceNumber == 2)
            ) {
                expectedMessageFound = true;
                EXPECT_EQ(100, messageSent.message.appendEntries.prevLogIndex);
                EXPECT_EQ(7, messageSent.message.appendEntries.prevLogTerm);
                ASSERT_EQ(0, messageSent.message.log.size());
            }
        }
        EXPECT_TRUE(expectedMessageFound);
    }

    TEST_F(ServerTests_Replication, LeaderNextAppendAfterSnapshotInstallation) {
        // Arrange
        mockLog->baseIndex = 100;
        mockLog->commitIndex = 100;
        mockLog->baseTerm = 7;
        mockLog->snapshot = Json::Object({
            {"foo", "bar"}
        });
        AppendNoOpEntry(8);
        BecomeLeader(8, false);
        while (!InstallSnapshotSent()) {
            ReceiveAppendEntriesResults(2, 8, 0, false);
        }

        // Act
        messagesSent.clear();
        Raft::Message message;
        message.type = Raft::Message::Type::InstallSnapshotResults;
        message.term = 8;
        message.installSnapshotResults.matchIndex = 100;
        server.ReceiveMessage(message.Serialize(), 2);
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        bool expectedMessageFound = false;
        for (const auto& messageSent: messagesSent) {
            if (
                (messageSent.message.type == Raft::Message::Type::AppendEntries)
                && (messageSent.receiverInstanceNumber == 2)
            ) {
                expectedMessageFound = true;
                EXPECT_EQ(100, messageSent.message.appendEntries.prevLogIndex);
                EXPECT_EQ(7, messageSent.message.appendEntries.prevLogTerm);
                ASSERT_EQ(1, messageSent.message.log.size());
                EXPECT_EQ(8, messageSent.message.log[0].term);
            }
        }
        EXPECT_TRUE(expectedMessageFound);
    }

    TEST_F(ServerTests_Replication, FollowerInstallSnapshot) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int term = 5;
        Raft::LogEntry entry;
        entry.term = term;
        mockLog->entries = {entry};
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(2, 4);

        // Act
        Raft::Message message;
        message.type = Raft::Message::Type::InstallSnapshot;
        message.term = term;
        message.seq = 77;
        message.installSnapshot.lastIncludedIndex = 100;
        message.installSnapshot.lastIncludedTerm = 3;
        message.snapshot = Json::Object({
            {"foo", "bar"},
        });
        server.ReceiveMessage(message.Serialize(), leaderId);
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(leaderId, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::InstallSnapshotResults, messagesSent[0].message.type);
        EXPECT_EQ(4, messagesSent[0].message.term);
        EXPECT_EQ(77, messagesSent[0].message.seq);
        EXPECT_EQ(100, messagesSent[0].message.installSnapshotResults.matchIndex);
        EXPECT_EQ(message.snapshot, snapshotInstalled);
        EXPECT_EQ(message.installSnapshot.lastIncludedIndex, lastIncludedIndexInSnapshot);
        EXPECT_EQ(message.installSnapshot.lastIncludedTerm, lastIncludedTermInSnapshot);
        EXPECT_EQ(100, mockLog->baseIndex);
        EXPECT_EQ(100, mockLog->commitIndex);
        EXPECT_EQ(3, mockLog->baseTerm);
        EXPECT_EQ(message.snapshot, mockLog->snapshot);
    }

    TEST_F(ServerTests_Replication, FollowerAppendEntriesAfterSnapshot) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int term = 5;
        Raft::LogEntry entry;
        entry.term = term;
        mockLog->entries = {entry};
        MobilizeServer();

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
        server.WaitForAtLeastOneWorkerLoop();
        messagesSent.clear();
        message.type = Raft::Message::Type::AppendEntries;
        message.term = term;
        message.appendEntries.leaderCommit = 100;
        message.appendEntries.prevLogIndex = 100;
        message.appendEntries.prevLogTerm = 3;
        server.ReceiveMessage(message.Serialize(), leaderId);
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(leaderId, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_EQ(100, messagesSent[0].message.appendEntriesResults.matchIndex);
        EXPECT_TRUE(messagesSent[0].message.appendEntriesResults.success);
    }

    TEST_F(ServerTests_Replication, FollowerDoNotInstallStaleSnapshot) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int term = 5;
        Raft::LogEntry entry;
        entry.term = term;
        const auto originalSnapshot = Json::Object({
            {"foo", "spam"},
        });
        mockLog->snapshot = originalSnapshot;
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(leaderId, term, 0, {entry, entry});
        ReceiveAppendEntriesFromMockLeader(leaderId, term, 1, {});
        messagesSent.clear();

        // Act
        Raft::Message message;
        message.type = Raft::Message::Type::InstallSnapshot;
        message.term = term;
        message.installSnapshot.lastIncludedIndex = 0;
        message.installSnapshot.lastIncludedTerm = 0;
        message.snapshot = Json::Object({
            {"foo", "bar"},
        });
        server.ReceiveMessage(message.Serialize(), leaderId);
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        ASSERT_EQ(1, messagesSent.size());
        EXPECT_EQ(leaderId, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::InstallSnapshotResults, messagesSent[0].message.type);
        EXPECT_EQ(2, messagesSent[0].message.installSnapshotResults.matchIndex);
        EXPECT_EQ(0, mockLog->baseIndex);
        EXPECT_EQ(1, mockLog->commitIndex);
        EXPECT_EQ(0, mockLog->baseTerm);
        EXPECT_EQ(originalSnapshot, mockLog->snapshot);
    }

    TEST_F(ServerTests_Replication, AppendEntriesShouldNotSendMessageToFollowerStillProcessingInstallSnapshot) {
        // Arrange
        mockLog->baseIndex = 100;
        mockLog->commitIndex = 100;
        mockLog->baseTerm = 7;
        mockLog->snapshot = Json::Object({
            {"foo", "bar"}
        });
        AppendNoOpEntry(8);
        BecomeLeader(8, false);
        while (!InstallSnapshotSent()) {
            ReceiveAppendEntriesResults(2, 8, 0, false);
        }

        // Act
        messagesSent.clear();
        Raft::LogEntry testEntry;
        testEntry.term = 8;
        server.AppendLogEntries({testEntry});
        server.WaitForAtLeastOneWorkerLoop();

        // Assert
        for (const auto& messageSent: messagesSent) {
            EXPECT_FALSE(
                (messageSent.receiverInstanceNumber == 2)
                && (messageSent.message.type == Raft::Message::Type::AppendEntries)
            );
        }
    }

}
