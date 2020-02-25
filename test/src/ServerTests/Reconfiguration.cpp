/**
 * @file Reconfiguration.cpp
 *
 * This module contains the unit tests of the Raft::Server class that have to
 * do with the reconfiguration process/aspects of the Raft Consensus algorithm.
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
    struct ServerTests_Reconfiguration
        : public Common
    {
    };

    TEST_F(ServerTests_Reconfiguration, DoNotCrashWhenCommittingSingleConfigFollowedByNoOp) {
        // Arrange
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;
        auto command = std::make_shared< Raft::SingleConfigurationCommand >();
        command->configuration.instanceIds = {2, 5, 6, 7};
        Raft::LogEntry firstEntry;
        firstEntry.term = 6;
        firstEntry.command = std::move(command);
        Raft::LogEntry secondEntry;
        secondEntry.term = 6;
        mockLog->entries = {
            std::move(firstEntry),
            std::move(secondEntry)
        };

        // Act
        BecomeLeader(6);

        // Assert
    }

    TEST_F(ServerTests_Reconfiguration, ApplyConfigNonVotingMemberSingleConfigOnStartup) {
        // Arrange
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;
        auto command = std::make_shared< Raft::SingleConfigurationCommand >();
        command->configuration.instanceIds = {5, 6, 7, 11};
        Raft::LogEntry entry;
        entry.term = 6;
        entry.command = std::move(command);
        mockLog->entries = {entry};

        // Act
        MobilizeServer();

        // Assert
        EXPECT_FALSE(server.IsVotingMember());
    }

    TEST_F(ServerTests_Reconfiguration, NonVotingMemberOnStartupNoLog) {
        // Arrange
        clusterConfiguration.instanceIds = {5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;

        // Act
        MobilizeServer();

        // Assert
        EXPECT_FALSE(server.IsVotingMember());
    }

    TEST_F(ServerTests_Reconfiguration, ApplyConfigNonVotingMemberSingleConfigWhenAppendedAsFollower) {
        // Arrange
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;
        auto command = std::make_shared< Raft::SingleConfigurationCommand >();
        command->configuration.instanceIds = {5, 6, 7, 11};
        Raft::LogEntry entry;
        entry.term = 6;
        entry.command = std::move(command);
        mockLog->entries = {entry};
        MobilizeServer();

        // Act
        ReceiveAppendEntriesFromMockLeader(5, 6);

        // Assert
        EXPECT_FALSE(server.IsVotingMember());
    }

    TEST_F(ServerTests_Reconfiguration, BecomeNonVotingMemberWhenLeaderAndSingleConfigAppendedAndNotInClusterAnymore) {
        // Arrange
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;
        auto command = std::make_shared< Raft::SingleConfigurationCommand >();
        command->configuration.instanceIds = {5, 6, 11};
        const auto newInstanceIds = command->configuration.instanceIds;
        Raft::LogEntry entry;
        constexpr int term = 6;
        entry.term = term;
        entry.command = std::move(command);
        BecomeLeader(term);

        // Act
        server.AppendLogEntries({entry});

        // Assert
        EXPECT_FALSE(server.IsVotingMember());
    }

    TEST_F(ServerTests_Reconfiguration, StepDownFromLeadershipOnceSingleConfigCommittedAndNotInClusterAnymore) {
        // Arrange
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;
        constexpr int term = 6;
        BecomeLeader(term);
        auto command = std::make_shared< Raft::SingleConfigurationCommand >();
        command->configuration.instanceIds = {5, 6, 11};
        const auto newInstanceIds = command->configuration.instanceIds;
        Raft::LogEntry entry;
        entry.term = term;
        entry.command = std::move(command);
        server.AppendLogEntries({entry});
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        scheduler->WakeUp();

        // Act
        for (const auto& messageSent: messagesSent) {
            if (messageSent.message.type == Raft::Message::Type::AppendEntries) {
                EXPECT_TRUE(
                    newInstanceIds.find(messageSent.receiverInstanceNumber)
                    != newInstanceIds.end()
                );
            }
        }
        size_t responseCount = 0;
        for (auto instance: newInstanceIds) {
            if (instance != serverConfiguration.selfInstanceId) {
                ReceiveAppendEntriesResults(instance, term, 1);
                ++responseCount;
                if (responseCount >= 2) {
                    EXPECT_EQ(
                        Raft::IServer::ElectionState::Follower,
                        server.GetElectionState()
                    ) << "responses: " << responseCount;
                } else {
                    EXPECT_EQ(
                        Raft::IServer::ElectionState::Leader,
                        server.GetElectionState()
                    ) << "responses: " << responseCount;
                }
            }
        }

        // Assert
    }

    TEST_F(ServerTests_Reconfiguration, SendCorrectHeartBeatToNewServersOnceJointConfigApplied) {
        // Arrange
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;
        constexpr int term = 6;
        BecomeLeader(term);
        auto command = std::make_shared< Raft::JointConfigurationCommand >();
        command->oldConfiguration.instanceIds = clusterConfiguration.instanceIds;
        command->newConfiguration.instanceIds = {2, 5, 6, 7, 11, 12};
        Raft::LogEntry entry;
        entry.term = term;
        entry.command = std::move(command);
        messagesSent.clear();

        // Act
        server.AppendLogEntries({entry});
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        scheduler->WakeUp();

        // Assert
        bool newServerReceivedHeartBeat = false;
        for (const auto& messageSent: messagesSent) {
            if (
                (messageSent.message.type == Raft::Message::Type::AppendEntries)
                && (messageSent.receiverInstanceNumber == 12)
            ) {
                messagesSent.clear();
                newServerReceivedHeartBeat = true;
                ReceiveAppendEntriesResults(12, term, 0, false);
                break;
            }
        }
        ASSERT_TRUE(newServerReceivedHeartBeat);
        bool newServerReceivedLog = false;
        for (const auto& messageSent: messagesSent) {
            if (
                (messageSent.message.type == Raft::Message::Type::AppendEntries)
                && (messageSent.receiverInstanceNumber == 12)
            ) {
                newServerReceivedLog = true;
                EXPECT_EQ(0, messageSent.message.appendEntries.prevLogIndex);
                ASSERT_EQ(1, messageSent.message.log.size());
                EXPECT_EQ(entry, messageSent.message.log[0]);
            }
        }
        EXPECT_TRUE(newServerReceivedLog);
    }

    TEST_F(ServerTests_Reconfiguration, VotingMemberFromBothConfigsJointConfig) {
        // Arrange
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;
        auto command = std::make_shared< Raft::JointConfigurationCommand >();
        command->oldConfiguration.instanceIds = {2, 5, 6, 7, 11};
        command->newConfiguration.instanceIds = {2, 5, 6, 7};
        Raft::LogEntry entry;
        entry.term = 6;
        entry.command = std::move(command);
        mockLog->entries = {entry};

        // Act
        MobilizeServer();

        // Assert
        EXPECT_TRUE(server.IsVotingMember());
    }

    TEST_F(ServerTests_Reconfiguration, VotingMemberFromNewConfigJointConfig) {
        // Arrange
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;
        auto command = std::make_shared< Raft::JointConfigurationCommand >();
        command->oldConfiguration.instanceIds = {5, 6, 7, 11};
        command->newConfiguration.instanceIds = {2, 5, 6, 7, 11};
        Raft::LogEntry entry;
        entry.term = 6;
        entry.command = std::move(command);
        mockLog->entries = {entry};

        // Act
        MobilizeServer();

        // Assert
        EXPECT_TRUE(server.IsVotingMember());
    }

    TEST_F(ServerTests_Reconfiguration, VotingMemberFromOldConfigJointConfig) {
        // Arrange
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;
        auto command = std::make_shared< Raft::JointConfigurationCommand >();
        command->oldConfiguration.instanceIds = {2, 5, 6, 7, 11};
        command->newConfiguration.instanceIds = {5, 6, 7, 11};
        Raft::LogEntry entry;
        entry.term = 6;
        entry.command = std::move(command);
        mockLog->entries = {entry};

        // Act
        MobilizeServer();

        // Assert
        EXPECT_TRUE(server.IsVotingMember());
    }

    TEST_F(ServerTests_Reconfiguration, NonVotingMemberJointConfig) {
        // Arrange
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;
        auto command = std::make_shared< Raft::JointConfigurationCommand >();
        command->oldConfiguration.instanceIds = {5, 6, 7, 11};
        command->newConfiguration.instanceIds = {5, 6, 7};
        Raft::LogEntry entry;
        entry.term = 6;
        entry.command = std::move(command);
        mockLog->entries = {entry};

        // Act
        MobilizeServer();

        // Assert
        EXPECT_FALSE(server.IsVotingMember());
    }

    TEST_F(ServerTests_Reconfiguration, NonVotingMemberShouldNotVoteForAnyCandidate) {
        // Arrange
        clusterConfiguration.instanceIds = {5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;
        MobilizeServer();

        // Act
        RequestVote(2, 1, 0);

        // Assert
        ASSERT_TRUE(messagesSent.empty());
    }

    TEST_F(ServerTests_Reconfiguration, NonVotingMemberShouldNotStartNewElection) {
        // Arrange
        clusterConfiguration.instanceIds = {5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;
        MobilizeServer();

        // Act
        WaitForElectionTimeout();

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
        EXPECT_TRUE(messagesSent.empty());
    }

    TEST_F(ServerTests_Reconfiguration, FollowerRevertConfigWhenRollingBackBeforeConfigChange) {
        // Arrange
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;
        auto singleConfigCommand = std::make_shared< Raft::SingleConfigurationCommand >();
        singleConfigCommand->oldConfiguration.instanceIds = {2, 5, 6, 7, 11};
        singleConfigCommand->configuration.instanceIds = {5, 6, 7, 11};
        Raft::LogEntry entry1, entry2;
        entry1.term = 6;
        entry1.command = std::move(singleConfigCommand);
        entry2.term = 7;
        mockLog->entries = {entry1};
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(5, 6, 1);

        // Act
        ReceiveAppendEntriesFromMockLeader(6, 7, 1, 0, {entry2});

        // Assert
        EXPECT_TRUE(server.IsVotingMember());
    }

    TEST_F(ServerTests_Reconfiguration, StartConfigurationProcessWhenLeader) {
        // Arrange
        constexpr int term = 5;
        serverConfiguration.selfInstanceId = 2;
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        Raft::ClusterConfiguration newConfiguration(clusterConfiguration);
        newConfiguration.instanceIds = {2, 5, 6, 7, 12};
        BecomeLeader(term);

        // Act
        server.ChangeConfiguration(newConfiguration);
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        scheduler->WakeUp();

        // Assert
        EXPECT_TRUE(server.HasJointConfiguration());
        std::map< int, bool > appendEntriesReceivedPerInstance;
        for (auto instanceNumber: clusterConfiguration.instanceIds) {
            appendEntriesReceivedPerInstance[instanceNumber] = false;
        }
        for (auto instanceNumber: newConfiguration.instanceIds) {
            appendEntriesReceivedPerInstance[instanceNumber] = false;
        }
        (void)appendEntriesReceivedPerInstance.erase(serverConfiguration.selfInstanceId);
        for (const auto& messageSent: messagesSent) {
            if (messageSent.message.type == Raft::Message::Type::AppendEntries) {
                auto appendEntriesReceivedPerInstanceEntry = appendEntriesReceivedPerInstance.find(messageSent.receiverInstanceNumber);
                EXPECT_FALSE(appendEntriesReceivedPerInstanceEntry == appendEntriesReceivedPerInstance.end());
                appendEntriesReceivedPerInstanceEntry->second = true;
            }
        }
        for (const auto& appendEntriesReceivedPerInstanceEntry: appendEntriesReceivedPerInstance) {
            EXPECT_TRUE(appendEntriesReceivedPerInstanceEntry.second)
                << "instance: " << appendEntriesReceivedPerInstanceEntry.first;
        }
    }

    TEST_F(ServerTests_Reconfiguration, StartConfigurationProcessWhenNotLeader) {
        // Arrange
        serverConfiguration.selfInstanceId = 2;
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        Raft::ClusterConfiguration newConfiguration(clusterConfiguration);
        newConfiguration.instanceIds = {2, 5, 6, 7, 12};
        MobilizeServer();

        // Act
        server.ChangeConfiguration(newConfiguration);
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        scheduler->WakeUp();

        // Assert
        EXPECT_FALSE(server.HasJointConfiguration());
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
        EXPECT_TRUE(messagesSent.empty());
    }

    TEST_F(ServerTests_Reconfiguration, DoNotApplyJointConfigurationIfNewServersAreNotYetCaughtUp) {
        // Arrange
        constexpr int term = 5;
        serverConfiguration.selfInstanceId = 2;
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        Raft::ClusterConfiguration newConfiguration(clusterConfiguration);
        newConfiguration.instanceIds = {2, 5, 6, 7, 12};
        const std::set< int > jointConfigurationNotIncludingSelfInstanceIds = {5, 6, 7, 11, 12};
        BecomeLeader(term);
        Raft::LogEntry entry;
        entry.term = term;
        server.AppendLogEntries({entry});
        for (auto instanceNumber: clusterConfiguration.instanceIds) {
            if (instanceNumber == serverConfiguration.selfInstanceId) {
                continue;
            }
            ReceiveAppendEntriesResults(instanceNumber, term, 1);
        }

        // Act
        server.ChangeConfiguration(newConfiguration);
        messagesSent.clear();
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        scheduler->WakeUp();

        // Assert
        EXPECT_TRUE(server.HasJointConfiguration());
        std::map< int, bool > appendEntriesReceivedPerInstance;
        for (auto instanceNumber: jointConfigurationNotIncludingSelfInstanceIds) {
            appendEntriesReceivedPerInstance[instanceNumber] = false;
        }
        for (const auto& messageSent: messagesSent) {
            if (messageSent.message.type == Raft::Message::Type::AppendEntries) {
                auto appendEntriesReceivedPerInstanceEntry = appendEntriesReceivedPerInstance.find(messageSent.receiverInstanceNumber);
                EXPECT_FALSE(appendEntriesReceivedPerInstanceEntry == appendEntriesReceivedPerInstance.end());
                appendEntriesReceivedPerInstanceEntry->second = true;
                EXPECT_EQ(1, messageSent.message.appendEntries.prevLogIndex);
                EXPECT_EQ(term, messageSent.message.appendEntries.prevLogTerm);
                EXPECT_TRUE(messageSent.message.log.empty());
            }
        }
        for (const auto& appendEntriesReceivedPerInstanceEntry: appendEntriesReceivedPerInstance) {
            EXPECT_TRUE(appendEntriesReceivedPerInstanceEntry.second)
                << "instance: " << appendEntriesReceivedPerInstanceEntry.first;
        }
    }

    TEST_F(ServerTests_Reconfiguration, ApplyJointConfigurationOnceNewServersCaughtUp) {
        // Arrange
        constexpr int term = 5;
        serverConfiguration.selfInstanceId = 2;
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        Raft::ClusterConfiguration newConfiguration(clusterConfiguration);
        newConfiguration.instanceIds = {2, 5, 6, 7, 12};
        const std::set< int > jointConfigurationNotIncludingSelfInstanceIds = {5, 6, 7, 11, 12};
        BecomeLeader(term);
        Raft::LogEntry entry;
        entry.term = term;
        server.AppendLogEntries({entry});
        for (auto instanceNumber: clusterConfiguration.instanceIds) {
            if (instanceNumber == serverConfiguration.selfInstanceId) {
                continue;
            }
            ReceiveAppendEntriesResults(instanceNumber, term, 1);
        }

        // Act
        server.ChangeConfiguration(newConfiguration);
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        scheduler->WakeUp();
        for (auto instanceNumber: jointConfigurationNotIncludingSelfInstanceIds) {
            ReceiveAppendEntriesResults(instanceNumber, term, 1);
        }
        messagesSent.clear();
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        scheduler->WakeUp();

        // Assert
        EXPECT_TRUE(server.HasJointConfiguration());
        std::map< int, bool > appendEntriesReceivedPerInstance;
        for (auto instanceNumber: jointConfigurationNotIncludingSelfInstanceIds) {
            appendEntriesReceivedPerInstance[instanceNumber] = false;
        }
        for (const auto& messageSent: messagesSent) {
            if (messageSent.message.type == Raft::Message::Type::AppendEntries) {
                auto appendEntriesReceivedPerInstanceEntry = appendEntriesReceivedPerInstance.find(messageSent.receiverInstanceNumber);
                EXPECT_FALSE(appendEntriesReceivedPerInstanceEntry == appendEntriesReceivedPerInstance.end());
                appendEntriesReceivedPerInstanceEntry->second = true;
                EXPECT_EQ(1, messageSent.message.appendEntries.prevLogIndex);
                EXPECT_EQ(term, messageSent.message.appendEntries.prevLogTerm);
                EXPECT_EQ(1, messageSent.message.log.size());
            }
        }
        for (const auto& appendEntriesReceivedPerInstanceEntry: appendEntriesReceivedPerInstance) {
            EXPECT_TRUE(appendEntriesReceivedPerInstanceEntry.second)
                << "instance: " << appendEntriesReceivedPerInstanceEntry.first;
        }
    }

    TEST_F(ServerTests_Reconfiguration, ApplyNewConfigurationOnceJointConfigurationCommitted) {
        // Arrange
        constexpr int term = 5;
        serverConfiguration.selfInstanceId = 2;
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        Raft::ClusterConfiguration newConfiguration(clusterConfiguration);
        newConfiguration.instanceIds = {2, 5, 6, 7, 12};
        const std::set< int > newConfigurationNotIncludingSelfInstanceIds = {5, 6, 7, 12};
        const std::set< int > jointConfigurationNotIncludingSelfInstanceIds = {5, 6, 7, 11, 12};
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
            if (instanceNumber == serverConfiguration.selfInstanceId) {
                continue;
            }
            ReceiveAppendEntriesResults(instanceNumber, term, 1);
        }
        messagesSent.clear();
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        scheduler->WakeUp();

        // Assert
        EXPECT_FALSE(server.HasJointConfiguration());
        std::map< int, bool > appendEntriesReceivedPerInstance;
        for (auto instanceNumber: newConfigurationNotIncludingSelfInstanceIds) {
            appendEntriesReceivedPerInstance[instanceNumber] = false;
        }
        for (const auto& messageSent: messagesSent) {
            if (messageSent.message.type == Raft::Message::Type::AppendEntries) {
                auto appendEntriesReceivedPerInstanceEntry = appendEntriesReceivedPerInstance.find(messageSent.receiverInstanceNumber);
                ASSERT_FALSE(appendEntriesReceivedPerInstanceEntry == appendEntriesReceivedPerInstance.end());
                appendEntriesReceivedPerInstanceEntry->second = true;
                EXPECT_EQ(1, messageSent.message.appendEntries.prevLogIndex);
                EXPECT_EQ(term, messageSent.message.appendEntries.prevLogTerm);
                ASSERT_EQ(1, messageSent.message.log.size());
                ASSERT_FALSE(messageSent.message.log[0].command == nullptr);
                const auto& command = messageSent.message.log[0].command;
                EXPECT_EQ("SingleConfiguration", command->GetType());
                const auto singleConfigurationCommand = std::static_pointer_cast< Raft::SingleConfigurationCommand >(command);
                EXPECT_EQ(
                    newConfiguration.instanceIds,
                    singleConfigurationCommand->configuration.instanceIds
                );
                EXPECT_EQ(
                    clusterConfiguration.instanceIds,
                    singleConfigurationCommand->oldConfiguration.instanceIds
                );
            }
        }
        for (const auto& appendEntriesReceivedPerInstanceEntry: appendEntriesReceivedPerInstance) {
            EXPECT_TRUE(appendEntriesReceivedPerInstanceEntry.second)
                << "instance: " << appendEntriesReceivedPerInstanceEntry.first;
        }
    }

    TEST_F(ServerTests_Reconfiguration, DoNotApplySingleConfigurationWhenJointConfigurationCommittedIfSingleConfigurationAlreadyApplied) {
        // Arrange
        constexpr int term = 5;
        serverConfiguration.selfInstanceId = 2;
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        Raft::ClusterConfiguration newConfiguration(clusterConfiguration);
        newConfiguration.instanceIds = {2, 5, 6, 7, 12};
        const std::set< int > newConfigurationNotIncludingSelfInstanceIds = {5, 6, 7, 12};
        const std::set< int > jointConfigurationNotIncludingSelfInstanceIds = {5, 6, 7, 11, 12};
        auto jointConfigCommand = std::make_shared< Raft::JointConfigurationCommand >();
        jointConfigCommand->oldConfiguration.instanceIds = clusterConfiguration.instanceIds;
        jointConfigCommand->newConfiguration.instanceIds = newConfiguration.instanceIds;
        auto singleConfigCommand = std::make_shared< Raft::SingleConfigurationCommand >();
        singleConfigCommand->oldConfiguration.instanceIds = clusterConfiguration.instanceIds;
        singleConfigCommand->configuration.instanceIds = newConfiguration.instanceIds;
        Raft::LogEntry jointConfigEntry, singleConfigEntry;
        jointConfigEntry.term = term;
        jointConfigEntry.command = std::move(jointConfigCommand);
        singleConfigEntry.term = term;
        singleConfigEntry.command = std::move(singleConfigCommand);
        mockLog->entries = {std::move(jointConfigEntry), std::move(singleConfigEntry)};
        BecomeLeader(term, false);

        // Act
        for (auto instanceNumber: jointConfigurationNotIncludingSelfInstanceIds) {
            if (instanceNumber == serverConfiguration.selfInstanceId) {
                continue;
            }
            ReceiveAppendEntriesResults(instanceNumber, term, 1);
        }
        messagesSent.clear();

        // Assert
        EXPECT_EQ(2, mockLog->entries.size());
    }

    TEST_F(ServerTests_Reconfiguration, JointConfigurationShouldBeCommittedIfMajorityAchievedByCommonServerRespondingLast) {
        // Arrange
        constexpr int term = 5;
        serverConfiguration.selfInstanceId = 2;
        clusterConfiguration.instanceIds = {2, 5, 6};
        Raft::ClusterConfiguration newConfiguration(clusterConfiguration);
        newConfiguration.instanceIds = {2, 5, 7};
        const std::set< int > newConfigurationNotIncludingSelfInstanceIds = {5, 7};
        const std::set< int > jointConfigurationNotIncludingSelfInstanceIds = {5, 6, 7};
        auto command = std::make_shared< Raft::JointConfigurationCommand >();
        command->oldConfiguration.instanceIds = clusterConfiguration.instanceIds;
        command->newConfiguration.instanceIds = newConfiguration.instanceIds;
        Raft::LogEntry entry;
        entry.term = term;
        entry.command = std::move(command);
        mockLog->entries = {entry};
        BecomeLeader(term, false);
        ReceiveAppendEntriesResults(7, term, 1);
        ReceiveAppendEntriesResults(6, term, 1);

        // Act
        ReceiveAppendEntriesResults(5, term, 1);

        // Assert
        EXPECT_EQ(1, server.GetCommitIndex());
    }

    TEST_F(ServerTests_Reconfiguration, LeaderStepsDownIfNotInNewConfigurationOnceItIsCommitted) {
        // Arrange
        constexpr int term = 5;
        serverConfiguration.selfInstanceId = 2;
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        Raft::ClusterConfiguration newConfiguration(clusterConfiguration);
        newConfiguration.instanceIds = {5, 6, 7, 12};
        auto command = std::make_shared< Raft::SingleConfigurationCommand >();
        command->oldConfiguration.instanceIds = clusterConfiguration.instanceIds;
        command->configuration.instanceIds = newConfiguration.instanceIds;
        Raft::LogEntry entry;
        entry.term = term;
        entry.command = std::move(command);
        mockLog->entries = {entry};
        BecomeLeader(term);

        // Act
        for (auto instanceNumber: newConfiguration.instanceIds) {
            ReceiveAppendEntriesResults(instanceNumber, term, 1);
        }
        messagesSent.clear();

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Follower,
            server.GetElectionState()
        );
        EXPECT_FALSE(server.IsVotingMember());
    }

    TEST_F(ServerTests_Reconfiguration, LeaderMaintainsLeadershipIfInNewConfigurationOnceItIsCommitted) {
        // Arrange
        constexpr int term = 5;
        serverConfiguration.selfInstanceId = 2;
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        Raft::ClusterConfiguration newConfiguration(clusterConfiguration);
        newConfiguration.instanceIds = {2, 5, 6, 7, 12};
        auto command = std::make_shared< Raft::SingleConfigurationCommand >();
        command->oldConfiguration.instanceIds = clusterConfiguration.instanceIds;
        command->configuration.instanceIds = newConfiguration.instanceIds;
        Raft::LogEntry entry;
        entry.term = term;
        entry.command = std::move(command);
        mockLog->entries = {entry};
        BecomeLeader(term);

        // Act
        for (auto instanceNumber: newConfiguration.instanceIds) {
            if (instanceNumber == serverConfiguration.selfInstanceId) {
                continue;
            }
            ReceiveAppendEntriesResults(instanceNumber, term, 1);
        }
        messagesSent.clear();

        // Assert
        EXPECT_EQ(
            Raft::IServer::ElectionState::Leader,
            server.GetElectionState()
        );
        EXPECT_TRUE(server.IsVotingMember());
    }

    TEST_F(ServerTests_Reconfiguration, RemobilizeShouldClearJointConfigurationState) {
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
        mockLog->entries = {entry};
        MobilizeServer();
        server.Demobilize();
        mockLog = std::make_shared< MockLog >();

        // Act
        mockPersistentState->variables.currentTerm = term - 1;
        MobilizeServer();
        WaitForElectionTimeout();

        // Assert
        for (const auto messageInfo: messagesSent) {
            ASSERT_FALSE(
                (messageInfo.message.type == Raft::Message::Type::RequestVote)
                && (messageInfo.receiverInstanceNumber == 11)
            );
        }
    }

    TEST_F(ServerTests_Reconfiguration, CallDelegateOnApplyConfiguration) {
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
        mockLog->entries = {entry};

        // Act
        MobilizeServer();

        // Assert
        ASSERT_FALSE(configApplied == nullptr);
        EXPECT_EQ(
            std::set< int >({2, 6, 7, 12}),
            configApplied->instanceIds
        );
    }

    TEST_F(ServerTests_Reconfiguration, CallDelegateOnCommitConfiguration) {
        // Arrange
        constexpr int term = 5;
        serverConfiguration.selfInstanceId = 2;
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        Raft::ClusterConfiguration newConfiguration(clusterConfiguration);
        newConfiguration.instanceIds = {2, 6, 7, 12};
        auto jointConfigCommand = std::make_shared< Raft::JointConfigurationCommand >();
        jointConfigCommand->oldConfiguration.instanceIds = clusterConfiguration.instanceIds;
        jointConfigCommand->newConfiguration.instanceIds = newConfiguration.instanceIds;
        Raft::LogEntry jointConfigEntry;
        jointConfigEntry.term = term;
        jointConfigEntry.command = std::move(jointConfigCommand);
        auto singleConfigCommand = std::make_shared< Raft::SingleConfigurationCommand >();
        singleConfigCommand->oldConfiguration.instanceIds = clusterConfiguration.instanceIds;
        singleConfigCommand->configuration.instanceIds = newConfiguration.instanceIds;
        Raft::LogEntry singleConfigEntry;
        singleConfigEntry.term = term;
        singleConfigEntry.command = std::move(singleConfigCommand);
        mockLog->entries = {
            std::move(jointConfigEntry),
            std::move(singleConfigEntry)
        };
        BecomeLeader(term, false);

        // Act
        EXPECT_TRUE(configCommitted == nullptr);
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance != serverConfiguration.selfInstanceId) {
                ReceiveAppendEntriesResults(instance, term, 2);
            }
        }

        // Assert
        ASSERT_FALSE(configCommitted == nullptr);
        EXPECT_EQ(2, commitLogIndex);
        EXPECT_EQ(
            std::set< int >({2, 6, 7, 12}),
            configCommitted->instanceIds
        );
    }

    TEST_F(ServerTests_Reconfiguration, DoNotCallCommitConfigurationDelegateWhenReplayingOldSingleConfigurationCommand) {
        // Arrange
        constexpr int term = 5;
        serverConfiguration.selfInstanceId = 2;
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        Raft::ClusterConfiguration newConfiguration(clusterConfiguration);
        newConfiguration.instanceIds = {2, 6, 7, 12};
        Raft::ClusterConfiguration nextConfiguration(clusterConfiguration);
        nextConfiguration.instanceIds = {2, 6, 7, 13};
        auto singleConfigCommand = std::make_shared< Raft::SingleConfigurationCommand >();
        singleConfigCommand->oldConfiguration.instanceIds = clusterConfiguration.instanceIds;
        singleConfigCommand->configuration.instanceIds = newConfiguration.instanceIds;
        Raft::LogEntry singleConfigEntry;
        singleConfigEntry.term = term;
        singleConfigEntry.command = std::move(singleConfigCommand);
        auto jointConfigCommand = std::make_shared< Raft::JointConfigurationCommand >();
        jointConfigCommand->oldConfiguration.instanceIds = newConfiguration.instanceIds;
        jointConfigCommand->newConfiguration.instanceIds = nextConfiguration.instanceIds;
        Raft::LogEntry jointConfigEntry;
        jointConfigEntry.term = term;
        jointConfigEntry.command = std::move(jointConfigCommand);
        mockLog->entries = {
            std::move(singleConfigEntry),
            std::move(jointConfigEntry),
        };
        BecomeLeader(term, false);

        // Act
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance != serverConfiguration.selfInstanceId) {
                ReceiveAppendEntriesResults(instance, term, 2);
            }
        }

        // Assert
        EXPECT_TRUE(configCommitted == nullptr);
    }

    TEST_F(ServerTests_Reconfiguration, InitializeInstanceInfoAddBackServerThatWasPreviouslyLeader) {
        // Arrange
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 2;
        Raft::ClusterConfiguration newConfiguration(clusterConfiguration);
        newConfiguration.instanceIds = {2, 6, 7, 11};
        MobilizeServer();
        auto jointConfigCommand = std::make_shared< Raft::JointConfigurationCommand >();
        jointConfigCommand->oldConfiguration.instanceIds = clusterConfiguration.instanceIds;
        jointConfigCommand->newConfiguration.instanceIds = newConfiguration.instanceIds;
        Raft::LogEntry jointConfigEntry;
        jointConfigEntry.term = 6;
        jointConfigEntry.command = std::move(jointConfigCommand);
        auto singleConfigCommand = std::make_shared< Raft::SingleConfigurationCommand >();
        singleConfigCommand->oldConfiguration.instanceIds = clusterConfiguration.instanceIds;
        singleConfigCommand->configuration.instanceIds = newConfiguration.instanceIds;
        Raft::LogEntry singleConfigEntry;
        singleConfigEntry.term = 6;
        singleConfigEntry.command = std::move(singleConfigCommand);
        ReceiveAppendEntriesFromMockLeader(5, 6, {jointConfigEntry, singleConfigEntry});

        // Act
        WaitForElectionTimeout();
        for (auto instance: newConfiguration.instanceIds) {
            CastVote(instance, 7, true);
        }
        messagesSent.clear();
        server.ChangeConfiguration(clusterConfiguration);
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
        scheduler->WakeUp();
        messagesSent.clear();
        ReceiveAppendEntriesResults(5, 7, 1, false);

        // Assert
        for (const auto& messageSent: messagesSent) {
            if (
                (messageSent.message.type == Raft::Message::Type::AppendEntries)
                && (messageSent.receiverInstanceNumber == 5)
            ) {
                EXPECT_EQ(1, messageSent.message.appendEntries.prevLogIndex);
            }
        }
    }

}
