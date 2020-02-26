/**
 * @file Replication.cpp
 *
 * This module contains the unit tests of the Raft::Server class that have
 * to do with replicating server state across the entire cluster.
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
    struct ServerTests_Replication
        : public Common
    {
    };

    TEST_F(ServerTests_Replication, Leader_Append_Log_Entry) {
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
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));
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

    TEST_F(ServerTests_Replication, Leader_Append_Log_Entry_First_After_Snapshot) {
        // Arrange
        mockLog->baseIndex = 100;
        mockLog->commitIndex = 100;
        mockLog->baseTerm = 2;
        BecomeLeader(3);
        Raft::LogEntry entry;
        entry.term = 3;

        // Act
        server.AppendLogEntries({entry});

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));
        for (const auto& messageSent: messagesSent) {
            if (messageSent.message.type == Raft::Message::Type::AppendEntries) {
                EXPECT_EQ(3, messageSent.message.term);
                EXPECT_EQ(100, messageSent.message.appendEntries.prevLogIndex);
                EXPECT_EQ(2, messageSent.message.appendEntries.prevLogTerm);
            }
        }
    }

    TEST_F(ServerTests_Replication, Follower_Append_Log_Entry) {
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

    TEST_F(ServerTests_Replication, Leader_Do_Not_Advance_Commit_Index_When_Majority_Of_Cluster_Has_Not_Yet_Applied_Log_Entry) {
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

        // Assert
        EXPECT_EQ(0, server.GetCommitIndex());
    }

    TEST_F(ServerTests_Replication, Leader_Advance_Commit_Index_When_Majority_Of_Cluster_Has_Applied_Log_Entry) {
        // Arrange
        BecomeLeader(7);
        Raft::LogEntry firstEntry;
        firstEntry.term = 6;
        Raft::LogEntry secondEntry;
        secondEntry.term = 7;
        server.AppendLogEntries({firstEntry});

        // Act
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));
        messagesSent.clear();
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance != serverConfiguration.selfInstanceId) {
                ReceiveAppendEntriesResults(instance, 7, 1);
                EXPECT_EQ(0, server.GetCommitIndex());
            }
        }
        server.AppendLogEntries({secondEntry});
        size_t successfulResponseCount = 0;
        size_t responseCount = 0;
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));
        messagesSent.clear();
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

    TEST_F(ServerTests_Replication, Follower_Advance_Commit_Index_When_Majority_Of_Cluster_Has_Applied_Log_Entry) {
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

    TEST_F(ServerTests_Replication, Append_Entries_When_Not_Leader) {
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

        // Assert
        EXPECT_FALSE(AwaitMessagesSent(1));
    }

    TEST_F(ServerTests_Replication, Initialize_Last_Index) {
        // Arrange
        AppendNoOpEntry(1);
        AppendNoOpEntry(1);

        // Act
        MobilizeServer();

        // Assert
        EXPECT_EQ(2, server.GetLastIndex());
    }

    TEST_F(ServerTests_Replication, Leader_Initial_Append_Entries_From_End_Of_Log) {
        // Arrange
        AppendNoOpEntry(3);
        AppendNoOpEntry(7);
        BecomeLeader(8);

        // Act
        mockTimeKeeper->currentTime += serverConfiguration.heartbeatInterval + 0.001;
        scheduler->WakeUp();
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));

        // Assert
        for (const auto& messageSent: messagesSent) {
            if (messageSent.message.type == Raft::Message::Type::AppendEntries) {
                EXPECT_EQ(2, messageSent.message.appendEntries.prevLogIndex);
                EXPECT_EQ(7, messageSent.message.appendEntries.prevLogTerm);
                EXPECT_TRUE(messageSent.message.log.empty());
            }
        }
    }

    TEST_F(ServerTests_Replication, Leader_Append_Older_Entries_After_Discovering_Follower_Is_Behind) {
        // Arrange
        AppendNoOpEntry(3);
        AppendNoOpEntry(7);
        AppendNoOpEntry(8);
        BecomeLeader(8);
        mockTimeKeeper->currentTime += serverConfiguration.heartbeatInterval + 0.001;
        scheduler->WakeUp();
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));
        messagesSent.clear();

        // Act
        ReceiveAppendEntriesResults(2, 8, 1, false);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(Raft::Message::Type::AppendEntries, messagesSent[0].message.type);
        EXPECT_EQ(1, messagesSent[0].message.appendEntries.prevLogIndex);
        EXPECT_EQ(3, messagesSent[0].message.appendEntries.prevLogTerm);
        ASSERT_EQ(2, messagesSent[0].message.log.size());
        EXPECT_EQ(7, messagesSent[0].message.log[0].term);
        EXPECT_EQ(8, messagesSent[0].message.log[1].term);
    }

    TEST_F(ServerTests_Replication, Leader_Append_First_Entry_Based_On_Snapshot_After_Discovering_Follower_Is_Behind) {
        // Arrange
        mockLog->baseIndex = 100;
        mockLog->commitIndex = 100;
        mockLog->baseTerm = 7;
        AppendNoOpEntry(3);
        BecomeLeader(8, false);

        // Act
        ReceiveAppendEntriesResults(2, 8, 100, false);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(Raft::Message::Type::AppendEntries, messagesSent[0].message.type);
        EXPECT_EQ(100, messagesSent[0].message.appendEntries.prevLogIndex);
        EXPECT_EQ(7, messagesSent[0].message.appendEntries.prevLogTerm);
        ASSERT_EQ(1, messagesSent[0].message.log.size());
        EXPECT_EQ(3, messagesSent[0].message.log[0].term);
    }

    TEST_F(ServerTests_Replication, Leader_Append_Only_Log_Entry_After_Discovering_Follower_Has_No_Log_At_All_Nope_No_Sir_I_Am_New_Please_Forgive_Me) {
        // Arrange
        AppendNoOpEntry(3);
        BecomeLeader(8, false);

        // Act
        ReceiveAppendEntriesResults(2, 8, 0, false);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(Raft::Message::Type::AppendEntries, messagesSent[0].message.type);
        EXPECT_EQ(0, messagesSent[0].message.appendEntries.prevLogIndex);
        EXPECT_EQ(0, messagesSent[0].message.appendEntries.prevLogTerm);
        ASSERT_EQ(1, messagesSent[0].message.log.size());
        EXPECT_EQ(3, messagesSent[0].message.log[0].term);
    }

    TEST_F(ServerTests_Replication, Append_Entries_Not_Sent_If_Last_Not_Yet_Acknowledged) {
        // Arrange
        Raft::LogEntry testEntry;
        testEntry.term = 2;
        BecomeLeader(2, false);

        // Act
        server.AppendLogEntries({testEntry});

        // Assert
        EXPECT_FALSE(AwaitMessagesSent(1));
    }

    TEST_F(ServerTests_Replication, Next_Index_Advanced_And_Next_Entry_Appended_After_Previous_Acknowledged) {
        // Arrange
        Raft::LogEntry secondEntry;
        secondEntry.term = 3;
        AppendNoOpEntry(2);
        BecomeLeader(3, false);

        // Act
        server.AppendLogEntries({secondEntry});
        ReceiveAppendEntriesResults(2, 3, 1);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
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

    TEST_F(ServerTests_Replication, Follower_Match_Index_Beyond_What_Leader_Has) {
        // Arrange
        AppendNoOpEntry(2);
        BecomeLeader(8);
        mockTimeKeeper->currentTime += serverConfiguration.heartbeatInterval + 0.001;
        scheduler->WakeUp();

        // Act
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));
        messagesSent.clear();
        ReceiveAppendEntriesResults(2, 8, 2, false);

        // Assert
        EXPECT_EQ(1, server.GetMatchIndex(2));
    }

    TEST_F(ServerTests_Replication, No_Heart_Beat_Should_Be_Sent_While_Previous_Append_Entries_Unacknowledged) {
        // Arrange
        serverConfiguration.minimumElectionTimeout = 0.5;
        serverConfiguration.maximumElectionTimeout = 0.6;
        serverConfiguration.heartbeatInterval = 0.3;
        serverConfiguration.rpcTimeout = 0.4;
        BecomeLeader();
        Raft::LogEntry testEntry;
        testEntry.term = 2;
        server.AppendLogEntries({testEntry});
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));
        messagesSent.clear();

        // Act
        mockTimeKeeper->currentTime += serverConfiguration.heartbeatInterval + 0.001;
        scheduler->WakeUp();

        // Assert
        EXPECT_FALSE(AwaitMessagesSent(1));
    }

    TEST_F(ServerTests_Replication, Retransmit_Unacknowledged_Append_Entries) {
        // Arrange
        Raft::LogEntry testEntry;
        testEntry.term = 3;
        BecomeLeader(3);

        // Act
        server.AppendLogEntries({testEntry});
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));
        messagesSent.clear();
        mockTimeKeeper->currentTime += serverConfiguration.rpcTimeout + 0.001;
        scheduler->WakeUp();

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));
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

    TEST_F(ServerTests_Replication, Retransmit_Unacknowledged_Install_Snapshot) {
        // Arrange
        serverConfiguration.minimumElectionTimeout = 0.6;
        serverConfiguration.maximumElectionTimeout = 0.7;
        serverConfiguration.heartbeatInterval = 0.5;
        serverConfiguration.rpcTimeout = 0.1;
        serverConfiguration.installSnapshotTimeout = 0.2;
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
        ASSERT_TRUE(AwaitMessagesSent(1));
        messagesSent.clear();

        // Act
        mockTimeKeeper->currentTime += serverConfiguration.rpcTimeout + 0.001;
        scheduler->WakeUp();
        ASSERT_FALSE(AwaitMessagesSent(1));
        mockTimeKeeper->currentTime += serverConfiguration.installSnapshotTimeout + 0.001;
        scheduler->WakeUp();

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        bool installSnapshotRetransmitSentAtInstallSnapshotTimeout = false;
        for (const auto& messageSent: messagesSent) {
            if (
                (messageSent.message.type == Raft::Message::Type::InstallSnapshot)
            ) {
                installSnapshotRetransmitSentAtInstallSnapshotTimeout = true;
            }
        }
        EXPECT_TRUE(installSnapshotRetransmitSentAtInstallSnapshotTimeout);
    }

    TEST_F(ServerTests_Replication, Ignore_Duplicate_Append_Entries_Results) {
        // Arrange
        AppendNoOpEntry(3);
        AppendNoOpEntry(7);
        AppendNoOpEntry(8);
        BecomeLeader(8);
        mockTimeKeeper->currentTime += serverConfiguration.heartbeatInterval + 0.001;
        scheduler->WakeUp();

        // Act
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));
        messagesSent.clear();
        ReceiveAppendEntriesResults(2, 8, 1, false, 3);
        ASSERT_TRUE(AwaitMessagesSent(1));
        messagesSent.clear();
        ReceiveAppendEntriesResults(2, 8, 1, false, 3);

        // Assert
        EXPECT_FALSE(AwaitMessagesSent(1));
    }

    TEST_F(ServerTests_Replication, Reinitialize_Volatile_Leader_State_After_Election) {
        // Arrange
        BecomeLeader(7);
        Raft::LogEntry testEntry;
        testEntry.term = 7;
        server.AppendLogEntries({testEntry});
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance != serverConfiguration.selfInstanceId) {
                ReceiveAppendEntriesResults(instance, 7, 1);
            }
        }

        // Act
        ReceiveAppendEntriesFromMockLeader(2, 8, 1, true);
        ASSERT_TRUE(AwaitElectionTimeout());
        CastVotes(9);

        // Assert
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance != serverConfiguration.selfInstanceId) {
                EXPECT_EQ(2, server.GetNextIndex(instance));
                EXPECT_EQ(0, server.GetMatchIndex(instance));
            }
        }
    }

    TEST_F(ServerTests_Replication, Follower_Receive_Append_Entries_Success) {
        // Arrange
        Raft::LogEntry oldConflictingEntry, newConflictingEntry, nextEntry;
        oldConflictingEntry.term = 6;
        newConflictingEntry.term = 7;
        nextEntry.term = 8;
        mockLog->entries = {oldConflictingEntry};
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(2, 8);

        // Act
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntries;
        message.term = 8;
        message.seq = 42;
        message.appendEntries.leaderCommit = 1;
        message.appendEntries.prevLogIndex = 0;
        message.appendEntries.prevLogTerm = mockLog->baseTerm;
        message.log = {newConflictingEntry, nextEntry};
        server.ReceiveMessage(message.Serialize(), 2);

        // Assert
        ASSERT_EQ(2, mockLog->entries.size());
        EXPECT_EQ(newConflictingEntry.term, mockLog->entries[0].term);
        EXPECT_EQ(nextEntry.term, mockLog->entries[1].term);
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_EQ(8, messagesSent[0].message.term);
        EXPECT_EQ(42, messagesSent[0].message.seq);
        EXPECT_TRUE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(2, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, Follower_Receive_Append_Entries_All_Old) {
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
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_TRUE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(1, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, Follower_Receive_Append_Entries_Failure_Old_Term) {
        // Arrange
        Raft::LogEntry oldConflictingEntry, nextEntry;
        oldConflictingEntry.term = 6;
        nextEntry.term = 8;
        mockLog->entries = {oldConflictingEntry};
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(2, 8);

        // Act
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntries;
        message.term = 8;
        message.appendEntries.leaderCommit = 1;
        message.appendEntries.prevLogIndex = 1;
        message.appendEntries.prevLogTerm = 7;
        message.log = {nextEntry};
        server.ReceiveMessage(message.Serialize(), 2);

        // Assert
        ASSERT_EQ(1, mockLog->entries.size());
        EXPECT_EQ(oldConflictingEntry.term, mockLog->entries[0].term);
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_EQ(8, messagesSent[0].message.term);
        EXPECT_FALSE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(0, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, Follower_Receive_Append_Entries_Failure_Previous_Not_Found_No_Entries) {
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

        // Assert
        ASSERT_EQ(0, mockLog->entries.size());
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_EQ(8, messagesSent[0].message.term);
        EXPECT_FALSE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(0, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, Follower_Receive_Append_Entries_Failure_Previous_Not_Found_But_Have_Older_Entry) {
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

        // Assert
        ASSERT_EQ(1, mockLog->entries.size());
        EXPECT_EQ(7, mockLog->entries[0].term);
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_FALSE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(1, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, Follower_Receive_Append_Entries_Failure_Mismatching_Term_Previous_Index) {
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

        // Assert
        ASSERT_EQ(2, mockLog->entries.size());
        EXPECT_EQ(7, mockLog->entries[0].term);
        EXPECT_EQ(7, mockLog->entries[0].term);
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_FALSE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(1, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, Persistent_State_Update_For_New_Term_When_Receive_Append_Entries_From_Newer_Term_Leader) {
        // Arrange
        mockPersistentState->variables.currentTerm = 4;
        MobilizeServer();

        // Act
        ReceiveAppendEntriesFromMockLeader(2, 5, 95, true);

        // Assert
        EXPECT_EQ(5, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests_Replication, Joint_Concensus_Is_Not_Achieved_Solely_From_Simple_Majority) {
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
        BecomeLeader(term, false);

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

    TEST_F(ServerTests_Replication, Stale_Append_Entries_Deserves_A_Failure_Response) {
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
        ASSERT_TRUE(AwaitMessagesSent(1));
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

    TEST_F(ServerTests_Replication, Receiving_Heart_Beats_Does_Not_Cause_Persistent_State_Saves) {
        // Arrange
        MobilizeServer();
        const auto numSavesAtStart = mockPersistentState->saveCount;

        // Act
        while (mockTimeKeeper->currentTime <= serverConfiguration.maximumElectionTimeout * 2) {
            ReceiveAppendEntriesFromMockLeader(2, 2);
            mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2;
            scheduler->WakeUp();
        }

        // Assert
        EXPECT_EQ(numSavesAtStart + 1, mockPersistentState->saveCount);
    }

    TEST_F(ServerTests_Replication, Ignore_Append_Entries_Same_Term_If_Leader) {
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
        EXPECT_FALSE(AwaitMessagesSent(1));
    }

    TEST_F(ServerTests_Replication, Do_Not_Tell_Log_Keeper_To_Commit_If_Commit_Index_Unchanged) {
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

    TEST_F(ServerTests_Replication, Do_Not_Commit_To_Log_Any_Entries_We_Do_Not_Have) {
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

    TEST_F(ServerTests_Replication, Commit_Log_Any_Entries_We_Have_When_Index_Less_Than_Base_Plus_Size) {
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

    TEST_F(ServerTests_Replication, Stale_Server_Should_Revert_To_Follower_When_Append_Entry_Results_Higher_Term_Received) {
        // Arrange
        constexpr int term = 7;
        BecomeLeader(term);
        mockTimeKeeper->currentTime += serverConfiguration.heartbeatInterval + 0.001;
        scheduler->WakeUp();
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));

        // Act
        ReceiveAppendEntriesResults(2, term + 1, 0, false);

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
        EXPECT_EQ(term + 1, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests_Replication, Do_Not_Retransmit_Requests_To_Servers_No_Longer_In_The_Cluster) {
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

        // Act
        server.AppendLogEntries({entry});

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(newConfiguration.instanceIds.size() - 1));
        for (const auto messageInfo: messagesSent) {
            EXPECT_NE(5, messageInfo.receiverInstanceNumber);
        }
    }

    TEST_F(ServerTests_Replication, New_Leader_Should_Send_Heart_Beats_Immediately) {
        // Arrange
        mockPersistentState->variables.currentTerm = 1;
        MobilizeServer();
        (void)AwaitElectionTimeout();
        messagesSent.clear();

        // Act
        CastVotes(2);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));
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

    TEST_F(ServerTests_Replication, Leader_Should_Send_Regular_Heartbeats) {
        // Arrange
        serverConfiguration.heartbeatInterval = 0.001;
        BecomeLeader();

        // Act
        mockTimeKeeper->currentTime += 0.0011;
        scheduler->WakeUp();

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));
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

    TEST_F(ServerTests_Replication, Heartbeat_No_Log_Entries_Since_Snapshot) {
        // Arrange
        mockLog->baseIndex = 100;
        mockLog->commitIndex = 100;
        mockLog->baseTerm = 42;
        serverConfiguration.heartbeatInterval = 0.001;
        BecomeLeader();

        // Act
        mockTimeKeeper->currentTime += 0.0011;
        scheduler->WakeUp();

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));
        for (const auto& messageSent: messagesSent) {
            if (messageSent.message.type == Raft::Message::Type::AppendEntries) {
                EXPECT_EQ(100, messageSent.message.appendEntries.prevLogIndex);
                EXPECT_EQ(42, messageSent.message.appendEntries.prevLogTerm);
            }
        }
    }

    TEST_F(ServerTests_Replication, Remobilize_Should_Reset_Last_Index_Cache) {
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
        server.Demobilize();
        mockLog = std::make_shared< MockLog >();

        // Act
        mockPersistentState->variables.currentTerm = term - 1;
        MobilizeServer();

        // Assert
        EXPECT_FALSE(mockLog->invalidEntryIndexed);
    }

    TEST_F(ServerTests_Replication, Measure_Broadcast_Time) {
        // Arrange
        //
        // Server IDs:  {2, 5, 6, 7, 11}
        // Leader:          ^
        BecomeLeader(1);
        server.ResetStatistics();
        mockTimeKeeper->currentTime += serverConfiguration.heartbeatInterval + 0.001;
        scheduler->WakeUp();
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));

        // Act
        // first instance (2):  0.001 seconds
        // second instance (5): (leader - no measurement)
        // third instance (6):  0.002 seconds
        // fourth instance (7): 0.003 seconds
        // fifth instance (11): 0.004 seconds
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance != serverConfiguration.selfInstanceId) {
                mockTimeKeeper->currentTime += 0.001;
                scheduler->WakeUp();
                ReceiveAppendEntriesResults(instance, 1, 1);
            }
        }

        // Assert
        const auto stats = server.GetStatistics();
        EXPECT_NEAR(0.001, (double)stats["minBroadcastTime"], 0.0001);
        EXPECT_NEAR(0.0025, (double)stats["avgBroadcastTime"], 0.0001);
        EXPECT_NEAR(0.004, (double)stats["maxBroadcastTime"], 0.0001);
    }

    TEST_F(ServerTests_Replication, Measure_Time_Between_Messages) {
        // Arrange
        //
        // Server IDs:  {2, 5, 6, 7, 11}
        // Leader:       ^
        // Us:              ^
        constexpr int leaderId = 2;
        constexpr int term = 5;
        MobilizeServer();
        mockTimeKeeper->currentTime = 42.0;
        scheduler->WakeUp();
        ReceiveAppendEntriesFromMockLeader(leaderId, term);

        // Act
        mockTimeKeeper->currentTime += 0.001;
        scheduler->WakeUp();
        ReceiveAppendEntriesFromMockLeader(leaderId, term);
        mockTimeKeeper->currentTime += 0.002;
        scheduler->WakeUp();
        ReceiveAppendEntriesFromMockLeader(leaderId, term);
        mockTimeKeeper->currentTime += 0.003;
        scheduler->WakeUp();
        ReceiveAppendEntriesFromMockLeader(leaderId, term);
        mockTimeKeeper->currentTime += 0.004;
        scheduler->WakeUp();
        ReceiveAppendEntriesFromMockLeader(leaderId, term);
        mockTimeKeeper->currentTime += 0.005;
        scheduler->WakeUp();

        // Assert
        const auto stats = server.GetStatistics();
        EXPECT_NEAR(0.001, (double)stats["minTimeBetweenLeaderMessages"], 0.0001);
        EXPECT_NEAR(0.0025, (double)stats["avgTimeBetweenLeaderMessages"], 0.0001);
        EXPECT_NEAR(0.004, (double)stats["maxTimeBetweenLeaderMessages"], 0.0001);
        EXPECT_NEAR(0.005, (double)stats["timeSinceLastLeaderMessage"], 0.0001);
    }

    TEST_F(ServerTests_Replication, Leader_Declare_Caught_Up_Once_Commit_Index_Reaches_Initial_Leader_Last_Index) {
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

        // Assert
        EXPECT_TRUE(caughtUp);
    }

    TEST_F(ServerTests_Replication, Leader_Must_Not_Declare_Caught_Up_If_Commit_Index_Has_Not_Yet_Reached_Initial_Leader_Last_Index_When_Base_Index_Non_Zero) {
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

        // Assert
        EXPECT_FALSE(caughtUp);
    }

    TEST_F(ServerTests_Replication, Follower_Declare_Caught_Up_Once_Commit_Index_Reaches_Initial_Leader_Last_Index) {
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
        ReceiveAppendEntriesFromMockLeader(leaderId, newTerm, 1, true);

        // Assert
        EXPECT_TRUE(caughtUp);
    }

    TEST_F(ServerTests_Replication, Commit_Index_Initialized_From_Log) {
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

    TEST_F(ServerTests_Replication, Last_Index_Initialized_From_Log) {
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

    TEST_F(ServerTests_Replication, Correct_Match_Index_In_Append_Entries_Results_Based_On_Snapshot) {
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
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_EQ(8, messagesSent[0].message.term);
        EXPECT_TRUE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(102, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, Do_Not_Rewind_Commit_Index_If_Leader_Commit_Is_Behind) {
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

    TEST_F(ServerTests_Replication, Follower_Receive_Append_Entries_When_Log_Empty_Based_On_Snapshot) {
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
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_EQ(0, messagesSent[0].message.term);
        EXPECT_TRUE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(101, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, Follower_Receive_Append_Entries_Fail_When_Log_Based_On_Snapshot) {
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

        // Assert
        EXPECT_EQ(0, mockLog->entries.size());
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_EQ(0, messagesSent[0].message.term);
        EXPECT_FALSE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(100, messagesSent[0].message.appendEntriesResults.matchIndex);
    }

    TEST_F(ServerTests_Replication, Leader_Install_Snapshot) {
        // Arrange
        mockLog->baseIndex = 100;
        mockLog->commitIndex = 100;
        mockLog->baseTerm = 7;
        mockLog->snapshot = Json::Object({
            {"foo", "bar"}
        });
        BecomeLeader(8, false);

        // Act
        ReceiveAppendEntriesResults(2, 8, 0, false);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(Raft::Message::Type::InstallSnapshot, messagesSent[0].message.type);
        EXPECT_EQ(100, messagesSent[0].message.installSnapshot.lastIncludedIndex);
        EXPECT_EQ(7, messagesSent[0].message.installSnapshot.lastIncludedTerm);
        EXPECT_EQ(mockLog->snapshot, messagesSent[0].message.snapshot);
    }

    TEST_F(ServerTests_Replication, Leader_Heartbeat_After_Snapshot_Installation) {
        // Arrange
        serverConfiguration.minimumElectionTimeout = 0.5;
        serverConfiguration.maximumElectionTimeout = 0.6;
        serverConfiguration.heartbeatInterval = 0.3;
        serverConfiguration.rpcTimeout = 0.4;
        mockLog->baseIndex = 100;
        mockLog->commitIndex = 100;
        mockLog->baseTerm = 7;
        mockLog->snapshot = Json::Object({
            {"foo", "bar"}
        });
        BecomeLeader(8, false);
        ReceiveAppendEntriesResults(2, 8, 0, false);
        ASSERT_TRUE(AwaitMessagesSent(1));
        messagesSent.clear();

        // Act
        Raft::Message message;
        message.type = Raft::Message::Type::InstallSnapshotResults;
        message.term = 8;
        message.installSnapshotResults.matchIndex = 100;
        server.ReceiveMessage(message.Serialize(), 2);
        mockTimeKeeper->currentTime += serverConfiguration.heartbeatInterval + 0.001;
        scheduler->WakeUp();

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
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

    TEST_F(ServerTests_Replication, Leader_Next_Append_After_Snapshot_Installation) {
        // Arrange
        serverConfiguration.minimumElectionTimeout = 0.5;
        serverConfiguration.maximumElectionTimeout = 0.6;
        serverConfiguration.heartbeatInterval = 0.3;
        serverConfiguration.rpcTimeout = 0.4;
        mockLog->baseIndex = 100;
        mockLog->commitIndex = 100;
        mockLog->baseTerm = 7;
        mockLog->snapshot = Json::Object({
            {"foo", "bar"}
        });
        AppendNoOpEntry(8);
        BecomeLeader(8, false);
        ReceiveAppendEntriesResults(2, 8, 0, false);
        ASSERT_TRUE(AwaitMessagesSent(1));
        messagesSent.clear();

        // Act
        messagesSent.clear();
        Raft::Message message;
        message.type = Raft::Message::Type::InstallSnapshotResults;
        message.term = 8;
        message.installSnapshotResults.matchIndex = 100;
        server.ReceiveMessage(message.Serialize(), 2);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
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

    TEST_F(ServerTests_Replication, Follower_Install_Snapshot) {
        // Arrange
        constexpr int leaderId = 2;
        constexpr int term = 5;
        Raft::LogEntry entry;
        entry.term = term;
        mockLog->entries = {entry};
        mockPersistentState->variables.currentTerm = 4;
        MobilizeServer();

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

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
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

    TEST_F(ServerTests_Replication, Follower_Append_Entries_After_Snapshot) {
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
        ASSERT_TRUE(AwaitMessagesSent(1));
        messagesSent.clear();
        message.type = Raft::Message::Type::AppendEntries;
        message.term = term;
        message.appendEntries.leaderCommit = 100;
        message.appendEntries.prevLogIndex = 100;
        message.appendEntries.prevLogTerm = 3;
        server.ReceiveMessage(message.Serialize(), leaderId);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(leaderId, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::AppendEntriesResults, messagesSent[0].message.type);
        EXPECT_EQ(100, messagesSent[0].message.appendEntriesResults.matchIndex);
        EXPECT_TRUE(messagesSent[0].message.appendEntriesResults.success);
    }

    TEST_F(ServerTests_Replication, Follower_Do_Not_Install_Stale_Snapshot) {
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
        ReceiveAppendEntriesFromMockLeader(leaderId, term, 0, {entry, entry}, true);
        ReceiveAppendEntriesFromMockLeader(leaderId, term, 1, {}, true);
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

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        EXPECT_EQ(leaderId, messagesSent[0].receiverInstanceNumber);
        EXPECT_EQ(Raft::Message::Type::InstallSnapshotResults, messagesSent[0].message.type);
        EXPECT_EQ(2, messagesSent[0].message.installSnapshotResults.matchIndex);
        EXPECT_EQ(0, mockLog->baseIndex);
        EXPECT_EQ(1, mockLog->commitIndex);
        EXPECT_EQ(0, mockLog->baseTerm);
        EXPECT_EQ(originalSnapshot, mockLog->snapshot);
    }

    TEST_F(ServerTests_Replication, Append_Entries_Should_Not_Send_Message_To_Follower_Still_Processing_Install_Snapshot) {
        // Arrange
        mockLog->baseIndex = 100;
        mockLog->commitIndex = 100;
        mockLog->baseTerm = 7;
        mockLog->snapshot = Json::Object({
            {"foo", "bar"}
        });
        AppendNoOpEntry(8);
        BecomeLeader(8, false);
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance != serverConfiguration.selfInstanceId) {
                if (instance == 2) {
                    ReceiveAppendEntriesResults(instance, 8, 0, false);
                } else {
                    ReceiveAppendEntriesResults(instance, 8, 101, true);
                }
            }
        }
        ASSERT_TRUE(AwaitMessagesSent(1));
        messagesSent.clear();

        // Act
        Raft::LogEntry testEntry;
        testEntry.term = 8;
        server.AppendLogEntries({testEntry});

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 2));
        EXPECT_FALSE(AwaitMessagesSent(clusterConfiguration.instanceIds.size() - 1));
        for (const auto& messageSent: messagesSent) {
            EXPECT_FALSE(
                (messageSent.receiverInstanceNumber == 2)
                && (messageSent.message.type == Raft::Message::Type::AppendEntries)
            );
        }
    }

}
