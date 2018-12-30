/**
 * @file ServerTests.cpp
 *
 * This module contains the unit tests of the
 * Raft::Server class.
 *
 * © 2018 by Richard Walters
 */

#include "../../src/Message.hpp"

#include <algorithm>
#include <condition_variable>
#include <functional>
#include <gtest/gtest.h>
#include <Json/Value.hpp>
#include <limits>
#include <mutex>
#include <Raft/ILog.hpp>
#include <Raft/IPersistentState.hpp>
#include <Raft/Server.hpp>
#include <Raft/TimeKeeper.hpp>
#include <stddef.h>
#include <SystemAbstractions/StringExtensions.hpp>
#include <vector>

namespace {

    /**
     * This is a fake time-keeper which is used to test the server.
     */
    struct MockTimeKeeper
        : public Raft::TimeKeeper
    {
        // Properties

        double currentTime = 0.0;
        std::vector< std::function< void() > > destructionDelegates;

        // Lifecycle

        ~MockTimeKeeper() {
            for (const auto& destructionDelegate: destructionDelegates) {
                destructionDelegate();
            }
        }
        MockTimeKeeper(const MockTimeKeeper&) = delete;
        MockTimeKeeper(MockTimeKeeper&&) = delete;
        MockTimeKeeper& operator=(const MockTimeKeeper&) = delete;
        MockTimeKeeper& operator=(MockTimeKeeper&&) = delete;

        // Methods

        MockTimeKeeper() = default;

        void RegisterDestructionDelegate(std::function< void() > destructionDelegate) {
            destructionDelegates.push_back(destructionDelegate);
        }

        // Http::TimeKeeper

        virtual double GetCurrentTime() override {
            return currentTime;
        }
    };

    /**
     * This is a fake log keeper which is used to test the server.
     */
    struct MockLog
        : public Raft::ILog
    {
        // Properties

        std::vector< Raft::LogEntry > entries;
        bool invalidEntryIndexed = false;
        size_t commitIndex = 0;
        std::vector< std::function< void() > > destructionDelegates;

        // Lifecycle

        ~MockLog() {
            for (const auto& destructionDelegate: destructionDelegates) {
                destructionDelegate();
            }
        }
        MockLog(const MockLog&) = delete;
        MockLog(MockLog&&) = delete;
        MockLog& operator=(const MockLog&) = delete;
        MockLog& operator=(MockLog&&) = delete;

        // Methods

        MockLog() = default;

        void RegisterDestructionDelegate(std::function< void() > destructionDelegate) {
            destructionDelegates.push_back(destructionDelegate);
        }

        // Raft::ILog

        virtual size_t GetSize() override {
            return entries.size();
        }

        virtual const Raft::LogEntry& operator[](size_t index) override {
            if (
                (index == 0)
                || (index > entries.size())
            ) {
                invalidEntryIndexed = true;
                static Raft::LogEntry outOfRangeReturnValue;
                return outOfRangeReturnValue;
            }
            return entries[index - 1];
        }

        virtual void RollBack(size_t index) override {
            entries.resize(index);
        }

        virtual void Append(const std::vector< Raft::LogEntry >& newEntries) override {
            std::copy(
                newEntries.begin(),
                newEntries.end(),
                std::back_inserter(entries)
            );
        }

        virtual void Commit(size_t index) override {
            commitIndex = index;
        }
    };

    /**
     * This is a fake persistent state keeper which is used to test the server.
     */
    struct MockPersistentState
        : public Raft::IPersistentState
    {
        // Properties

        Raft::IPersistentState::Variables variables;
        std::vector< std::function< void() > > destructionDelegates;

        // Lifecycle

        ~MockPersistentState() {
            for (const auto& destructionDelegate: destructionDelegates) {
                destructionDelegate();
            }
        }
        MockPersistentState(const MockPersistentState&) = delete;
        MockPersistentState(MockPersistentState&&) = delete;
        MockPersistentState& operator=(const MockPersistentState&) = delete;
        MockPersistentState& operator=(MockPersistentState&&) = delete;

        // Methods

        MockPersistentState() = default;

        void RegisterDestructionDelegate(std::function< void() > destructionDelegate) {
            destructionDelegates.push_back(destructionDelegate);
        }

        // Raft::IPersistentState

        virtual Variables Load() override {
            return variables;
        }

        virtual void Save(const Variables& newVariables) override {
            variables = newVariables;
        }
    };

    /**
     * This holds information about a message received from the unit under
     * test.
     */
    struct MessageInfo {
        int receiverInstanceNumber;
        Raft::Message message;
    };

}

/**
 * This is the test fixture for these tests, providing common
 * setup and teardown for each test.
 */
struct ServerTests
    : public ::testing::Test
{
    // Properties

    Raft::Server server;
    Raft::Server::ClusterConfiguration clusterConfiguration;
    Raft::Server::ServerConfiguration serverConfiguration;
    std::vector< std::string > diagnosticMessages;
    SystemAbstractions::DiagnosticsSender::UnsubscribeDelegate diagnosticsUnsubscribeDelegate;
    std::shared_ptr< MockTimeKeeper > mockTimeKeeper = std::make_shared< MockTimeKeeper >();
    std::shared_ptr< MockLog > mockLog = std::make_shared< MockLog >();
    std::shared_ptr< MockPersistentState > mockPersistentState = std::make_shared< MockPersistentState >();
    std::vector< MessageInfo > messagesSent;

    // Methods

    void ServerSentMessage(
        const std::string& message,
        int receiverInstanceNumber
    ) {
        MessageInfo messageInfo;
        messageInfo.message = message;
        messageInfo.receiverInstanceNumber = receiverInstanceNumber;
        messagesSent.push_back(std::move(messageInfo));
    }

    void MobilizeServer() {
        server.Mobilize(
            mockLog,
            mockPersistentState,
            clusterConfiguration,
            serverConfiguration
        );
        server.WaitForAtLeastOneWorkerLoop();
    }

    void BecomeLeader(int term = 1) {
        mockPersistentState->variables.currentTerm = term - 1;
        MobilizeServer();
        mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
        server.WaitForAtLeastOneWorkerLoop();
        for (auto instance: clusterConfiguration.instanceIds) {
            if (instance != serverConfiguration.selfInstanceId) {
                Raft::Message message;
                message.type = Raft::Message::Type::RequestVoteResults;
                message.requestVoteResults.term = term;
                message.requestVoteResults.voteGranted = true;
                server.ReceiveMessage(message.Serialize(), instance);
            }
        }
        server.WaitForAtLeastOneWorkerLoop();
        messagesSent.clear();
    }

    void BecomeFollower(
        int leaderId,
        int term
    ) {
        MobilizeServer();
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntries;
        message.appendEntries.term = term;
        server.ReceiveMessage(message.Serialize(), leaderId);
        server.WaitForAtLeastOneWorkerLoop();
        messagesSent.clear();
    }

    void BecomeCandidate(int term = 1) {
        mockPersistentState->variables.currentTerm = term - 1;
        MobilizeServer();
        mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
        server.WaitForAtLeastOneWorkerLoop();
        messagesSent.clear();
    }

    void SetServerDelegates() {
        diagnosticsUnsubscribeDelegate = server.SubscribeToDiagnostics(
            [this](
                std::string senderName,
                size_t level,
                std::string message
            ){
                diagnosticMessages.push_back(
                    SystemAbstractions::sprintf(
                        "%s[%zu]: %s",
                        senderName.c_str(),
                        level,
                        message.c_str()
                    )
                );
            },
            0
        );
        server.SetTimeKeeper(mockTimeKeeper);
        server.SetSendMessageDelegate(
            [this](
                const std::string& message,
                int receiverInstanceNumber
            ){
                ServerSentMessage(message, receiverInstanceNumber);
            }
        );
    }

    // ::testing::Test

    virtual void SetUp() {
        SetServerDelegates();
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 5;
        serverConfiguration.minimumElectionTimeout = 0.1;
        serverConfiguration.maximumElectionTimeout = 0.2;
        serverConfiguration.rpcTimeout = 0.01;
        mockPersistentState->variables.currentTerm = 0;
        mockPersistentState->variables.votedThisTerm = false;
    }

    virtual void TearDown() {
        server.Demobilize();
        diagnosticsUnsubscribeDelegate();
    }
};

TEST_F(ServerTests, MobilizeTwiceDoesNotCrash) {
    // Arrange
    MobilizeServer();

    // Act
    MobilizeServer();
}

TEST_F(ServerTests, LogKeeperReleasedOnDemobilize) {
    // Arrange
    bool logDestroyed = false;
    const auto onLogDestroyed = [&logDestroyed]{
        logDestroyed = true;
    };
    mockLog->RegisterDestructionDelegate(onLogDestroyed);
    MobilizeServer();
    mockLog = nullptr;

    // Act
    server.Demobilize();

    // Assert
    EXPECT_TRUE(logDestroyed);
}

TEST_F(ServerTests, PersistentStateReleasedOnDemobilize) {
    // Arrange
    bool persistentStateDestroyed = false;
    const auto onPersistentStateDestroyed = [&persistentStateDestroyed]{
        persistentStateDestroyed = true;
    };
    mockPersistentState->RegisterDestructionDelegate(onPersistentStateDestroyed);
    MobilizeServer();
    mockPersistentState = nullptr;

    // Act
    server.Demobilize();

    // Assert
    EXPECT_TRUE(persistentStateDestroyed);
}

TEST_F(ServerTests, TimeKeeperReleasedOnDestruction) {
    // Arrange
    bool timeKeeperDestroyed = false;
    const auto onTimeKeeperDestroyed = [&timeKeeperDestroyed]{
        timeKeeperDestroyed = true;
    };
    mockTimeKeeper->RegisterDestructionDelegate(onTimeKeeperDestroyed);
    MobilizeServer();
    mockTimeKeeper = nullptr;

    // Act
    server.Demobilize();
    server = Raft::Server();

    // Assert
    EXPECT_TRUE(timeKeeperDestroyed);
}

TEST_F(ServerTests, ElectionNeverStartsBeforeMinimumTimeoutInterval) {
    // Arrange
    MobilizeServer();

    // Act
    mockTimeKeeper->currentTime = serverConfiguration.minimumElectionTimeout - 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(0, messagesSent.size());
}

TEST_F(ServerTests, ElectionAlwaysStartedWithinMaximumTimeoutInterval) {
    // Arrange
    MobilizeServer();

    // Act
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Candidate,
        server.GetElectionState()
    );
    EXPECT_EQ(4, messagesSent.size());
    for (const auto messageInfo: messagesSent) {
        EXPECT_EQ(
            Raft::Message::Type::RequestVote,
            messageInfo.message.type
        );
        EXPECT_EQ(1, messageInfo.message.requestVote.term);
    }
}

TEST_F(ServerTests, ElectionStartsAfterRandomIntervalBetweenMinimumAndMaximum) {
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

TEST_F(ServerTests, RequestVoteNotSentToAllServersExceptSelf) {
    // Arrange
    MobilizeServer();

    // Act
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(4, messagesSent.size());
    std::set< int > instances(
        clusterConfiguration.instanceIds.begin(),
        clusterConfiguration.instanceIds.end()
    );
    for (const auto messageInfo: messagesSent) {
        (void)instances.erase(messageInfo.receiverInstanceNumber);
    }
    EXPECT_EQ(
        std::set< int >{ serverConfiguration.selfInstanceId },
        instances
    );
}

TEST_F(ServerTests, RequestVoteIncludesLastIndex) {
    // Arrange
    MobilizeServer();
    server.SetLastIndex(42);

    // Act
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    for (const auto messageInfo: messagesSent) {
        EXPECT_EQ(
            42,
            messageInfo.message.requestVote.lastLogIndex
        );
    }
}

TEST_F(ServerTests, RequestVoteIncludesLastTermWithLog) {
    // Arrange
    Raft::LogEntry firstEntry;
    firstEntry.term = 3;
    mockLog->entries.push_back(std::move(firstEntry));
    Raft::LogEntry secondEntry;
    secondEntry.term = 7;
    mockLog->entries.push_back(std::move(secondEntry));
    MobilizeServer();

    // Act
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    for (const auto messageInfo: messagesSent) {
        EXPECT_EQ(
            7,
            messageInfo.message.requestVote.lastLogTerm
        );
    }
}

TEST_F(ServerTests, RequestVoteIncludesLastTermWithoutLog) {
    // Arrange
    MobilizeServer();

    // Act
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    for (const auto messageInfo: messagesSent) {
        EXPECT_EQ(
            0,
            messageInfo.message.requestVote.lastLogTerm
        );
    }
    EXPECT_FALSE(mockLog->invalidEntryIndexed);
}

TEST_F(ServerTests, ServerVotesForItselfInElectionItStarts) {
    // Arrange
    MobilizeServer();

    // Act
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(4, messagesSent.size());
    for (auto messageInfo: messagesSent) {
        EXPECT_EQ(5, messageInfo.message.requestVote.candidateId);
    }
}

TEST_F(ServerTests, ServerIncrementsTermInElectionItStarts) {
    // Arrange
    MobilizeServer();

    // Act
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(4, messagesSent.size());
    for (const auto messageInfo: messagesSent) {
        EXPECT_EQ(1, messageInfo.message.requestVote.term);
    }
}

TEST_F(ServerTests, ServerDoesReceiveUnanimousVoteInElection) {
    // Arrange
    MobilizeServer();
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    for (auto instance: clusterConfiguration.instanceIds) {
        if (instance != serverConfiguration.selfInstanceId) {
            Raft::Message message;
            message.type = Raft::Message::Type::RequestVoteResults;
            message.requestVoteResults.term = 1;
            message.requestVoteResults.voteGranted = true;
            server.ReceiveMessage(message.Serialize(), instance);
        }
    }
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Leader,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, ServerDoesReceiveNonUnanimousMajorityVoteInElection) {
    // Arrange
    MobilizeServer();
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    for (auto instance: clusterConfiguration.instanceIds) {
        if (instance != serverConfiguration.selfInstanceId) {
            Raft::Message message;
            message.type = Raft::Message::Type::RequestVoteResults;
            if (instance == 11) {
                message.requestVoteResults.term = 1;
                message.requestVoteResults.voteGranted = false;
            } else {
                message.requestVoteResults.term = 1;
                message.requestVoteResults.voteGranted = true;
            }
            server.ReceiveMessage(message.Serialize(), instance);
        }
    }
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Leader,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, ServerRetransmitsRequestVoteForSlowVotersInElection) {
    // Arrange
    MobilizeServer();
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: clusterConfiguration.instanceIds) {
        switch (instance) {
            case 6:
            case 7:
            case 11: {
                Raft::Message message;
                message.type = Raft::Message::Type::RequestVoteResults;
                if (instance == 11) {
                    message.requestVoteResults.term = 1;
                    message.requestVoteResults.voteGranted = false;
                } else {
                    message.requestVoteResults.term = 1;
                    message.requestVoteResults.voteGranted = true;
                }
                server.ReceiveMessage(message.Serialize(), instance);
            }
        }
    }
    server.WaitForAtLeastOneWorkerLoop();

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

TEST_F(ServerTests, ServerDoesNotRetransmitTooQuickly) {
    // Arrange
    MobilizeServer();
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: clusterConfiguration.instanceIds) {
        switch (instance) {
            case 6:
            case 7:
            case 11: {
                Raft::Message message;
                message.type = Raft::Message::Type::RequestVoteResults;
                if (instance == 11) {
                    message.requestVoteResults.term = 1;
                    message.requestVoteResults.voteGranted = false;
                } else {
                    message.requestVoteResults.term = 1;
                    message.requestVoteResults.voteGranted = true;
                }
                server.ReceiveMessage(message.Serialize(), instance);
            }
        }
    }
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    messagesSent.clear();
    mockTimeKeeper->currentTime += serverConfiguration.rpcTimeout - 0.0001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(0, messagesSent.size());
}

TEST_F(ServerTests, ServerRegularRetransmissions) {
    // Arrange
    MobilizeServer();
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: clusterConfiguration.instanceIds) {
        switch (instance) {
            case 6:
            case 7:
            case 11: {
                Raft::Message message;
                message.type = Raft::Message::Type::RequestVoteResults;
                if (instance == 11) {
                    message.requestVoteResults.term = 1;
                    message.requestVoteResults.voteGranted = false;
                } else {
                    message.requestVoteResults.term = 1;
                    message.requestVoteResults.voteGranted = true;
                }
                server.ReceiveMessage(message.Serialize(), instance);
            }
        }
    }
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    messagesSent.clear();
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout + serverConfiguration.rpcTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    EXPECT_EQ(1, messagesSent.size());
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout + serverConfiguration.rpcTimeout * 2 - 0.001;
    server.WaitForAtLeastOneWorkerLoop();
    EXPECT_EQ(1, messagesSent.size());
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout + serverConfiguration.rpcTimeout * 2 + 0.001;
    server.WaitForAtLeastOneWorkerLoop();
    EXPECT_EQ(2, messagesSent.size());
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout + serverConfiguration.rpcTimeout * 3 - 0.001;
    server.WaitForAtLeastOneWorkerLoop();
    EXPECT_EQ(2, messagesSent.size());
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout + serverConfiguration.rpcTimeout * 3 + 0.003;
    server.WaitForAtLeastOneWorkerLoop();
    EXPECT_EQ(3, messagesSent.size());

    // Assert
    for (const auto& messageSent: messagesSent) {
        EXPECT_EQ(2, messageSent.receiverInstanceNumber);
        EXPECT_EQ(
            Raft::Message::Type::RequestVote,
            messageSent.message.type
        );
        EXPECT_EQ(
            serverConfiguration.selfInstanceId,
            messageSent.message.requestVote.candidateId
        );
        EXPECT_EQ(
            1,
            messageSent.message.requestVote.term
        );
    }
}

TEST_F(ServerTests, ServerDoesNotReceiveAnyVotesInElection) {
    // Arrange
    MobilizeServer();
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    for (auto instance: clusterConfiguration.instanceIds) {
        if (instance != serverConfiguration.selfInstanceId) {
            Raft::Message message;
            message.type = Raft::Message::Type::RequestVoteResults;
            message.requestVoteResults.term = 1;
            message.requestVoteResults.voteGranted = false;
            server.ReceiveMessage(message.Serialize(), instance);
        }
    }
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Candidate,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, ServerAlmostWinsElection) {
    // Arrange
    MobilizeServer();
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    for (auto instance: clusterConfiguration.instanceIds) {
        if (instance != serverConfiguration.selfInstanceId) {
            Raft::Message message;
            message.type = Raft::Message::Type::RequestVoteResults;
            if (
                (instance == 2)
                || (instance == 6)
                || (instance == 11)
            ) {
                message.requestVoteResults.term = 1;
                message.requestVoteResults.voteGranted = false;
            } else {
                message.requestVoteResults.term = 1;
                message.requestVoteResults.voteGranted = true;
            }
            server.ReceiveMessage(message.Serialize(), instance);
        }
    }
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Candidate,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, TimeoutBeforeMajorityVoteOrNewLeaderHeartbeat) {
    // Arrange
    MobilizeServer();
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: clusterConfiguration.instanceIds) {
        if (instance != serverConfiguration.selfInstanceId) {
            Raft::Message message;
            message.type = Raft::Message::Type::RequestVoteResults;
            message.requestVoteResults.term = 1;
            message.requestVoteResults.voteGranted = false;
            server.ReceiveMessage(message.Serialize(), instance);
        }
    }
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();

    // Act
    mockTimeKeeper->currentTime += serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    for (const auto messageInfo: messagesSent) {
        EXPECT_EQ(
            Raft::Message::Type::RequestVote,
            messageInfo.message.type
        );
        EXPECT_EQ(2, messageInfo.message.requestVote.term);
    }
}

TEST_F(ServerTests, ReceiveVoteRequestWhenSameTermNoVotePending) {
    // Arrange
    MobilizeServer();

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 0;
    message.requestVote.candidateId = 2;
    message.requestVote.lastLogTerm = 999;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(
        Raft::Message::Type::RequestVoteResults,
        messagesSent[0].message.type
    );
    EXPECT_EQ(0, messagesSent[0].message.requestVoteResults.term);
    EXPECT_TRUE(messagesSent[0].message.requestVoteResults.voteGranted);
}

TEST_F(ServerTests, ReceiveVoteRequestWhenOurLogIsGreaterTerm) {
    // Arrange
    mockPersistentState->variables.currentTerm = 2;
    Raft::LogEntry firstEntry;
    firstEntry.term = 2;
    mockLog->entries.push_back(std::move(firstEntry));
    MobilizeServer();

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 3;
    message.requestVote.candidateId = 2;
    message.requestVote.lastLogTerm = 1;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(
        Raft::Message::Type::RequestVoteResults,
        messagesSent[0].message.type
    );
    EXPECT_EQ(3, messagesSent[0].message.requestVoteResults.term);
    EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
}

TEST_F(ServerTests, ReceiveVoteRequestWhenOurLogIsSameTermGreaterIndex) {
    // Arrange
    mockPersistentState->variables.currentTerm = 2;
    Raft::LogEntry firstEntry;
    firstEntry.term = 1;
    mockLog->entries.push_back(std::move(firstEntry));
    Raft::LogEntry secondEntry;
    secondEntry.term = 1;
    mockLog->entries.push_back(std::move(secondEntry));
    MobilizeServer();

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 3;
    message.requestVote.candidateId = 2;
    message.requestVote.lastLogTerm = 1;
    message.requestVote.lastLogIndex = 1;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(
        Raft::Message::Type::RequestVoteResults,
        messagesSent[0].message.type
    );
    EXPECT_EQ(3, messagesSent[0].message.requestVoteResults.term);
    EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
}

TEST_F(ServerTests, ReceiveVoteRequestWhenSameTermAlreadyVotedForAnother) {
    // Arrange
    MobilizeServer();
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 1;
    message.requestVote.candidateId = 2;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();

    // Act
    message = Raft::Message();
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 1;
    message.requestVote.candidateId = 6;
    server.ReceiveMessage(message.Serialize(), 6);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(
        Raft::Message::Type::RequestVoteResults,
        messagesSent[0].message.type
    );
    EXPECT_EQ(1, messagesSent[0].message.requestVoteResults.term);
    EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
}

TEST_F(ServerTests, ReceiveVoteRequestWhenSameTermAlreadyVotedForSame) {
    // Arrange
    MobilizeServer();
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 1;
    message.requestVote.candidateId = 2;
    message.requestVote.lastLogTerm = 999;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();

    // Act
    message = Raft::Message();
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 1;
    message.requestVote.candidateId = 2;
    message.requestVote.lastLogTerm = 999;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(
        Raft::Message::Type::RequestVoteResults,
        messagesSent[0].message.type
    );
    EXPECT_EQ(1, messagesSent[0].message.requestVoteResults.term);
    EXPECT_TRUE(messagesSent[0].message.requestVoteResults.voteGranted);
}

TEST_F(ServerTests, ReceiveVoteRequestLesserTerm) {
    // Arrange
    mockPersistentState->variables.currentTerm = 1;
    MobilizeServer();

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 0;
    message.requestVote.candidateId = 2;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(
        Raft::Message::Type::RequestVoteResults,
        messagesSent[0].message.type
    );
    EXPECT_EQ(1, messagesSent[0].message.requestVoteResults.term);
    EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
}

TEST_F(ServerTests, ReceiveVoteRequestGreaterTermWhenFollower) {
    // Arrange
    MobilizeServer();
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 1;
    message.requestVote.candidateId = 2;
    message.requestVote.lastLogTerm = 999;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(
        Raft::Message::Type::RequestVoteResults,
        messagesSent[0].message.type
    );
    EXPECT_EQ(1, messagesSent[0].message.requestVoteResults.term);
    EXPECT_TRUE(messagesSent[0].message.requestVoteResults.voteGranted);
}

TEST_F(ServerTests, ReceiveVoteRequestGreaterTermWhenCandidate) {
    // Arrange
    MobilizeServer();
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 2;
    message.requestVote.candidateId = 2;
    message.requestVote.lastLogTerm = 999;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout - 0.001;
    server.WaitForAtLeastOneWorkerLoop();

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

TEST_F(ServerTests, ReceiveVoteRequestGreaterTermWhenLeader) {
    // Arrange
    BecomeLeader();

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 2;
    message.requestVote.candidateId = 2;
    message.requestVote.lastLogTerm = 999;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();

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

TEST_F(ServerTests, DoNotStartVoteWhenAlreadyLeader) {
    // Arrange
    BecomeLeader();

    // Arrange
    mockTimeKeeper->currentTime += serverConfiguration.maximumElectionTimeout + 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    for (const auto& messageSent: messagesSent) {
        EXPECT_NE(
            Raft::Message::Type::RequestVote,
            messageSent.message.type
        );
    }
}

TEST_F(ServerTests, AfterRevertToFollowerDoNotStartNewElectionBeforeMinimumTimeout) {
    // Arrange
    BecomeLeader();
    mockTimeKeeper->currentTime += serverConfiguration.maximumElectionTimeout * 5;
    messagesSent.clear();
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 2;
    message.requestVote.candidateId = 2;
    message.requestVote.lastLogTerm = 999;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();

    // Act
    mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout - 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(0, messagesSent.size());
}

TEST_F(ServerTests, LeaderShouldRevertToFollowerWhenGreaterTermHeartbeatReceived) {
    // Arrange
    BecomeLeader();

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = 2;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Follower,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, CandidateShouldRevertToFollowerWhenGreaterTermHeartbeatReceived) {
    // Arrange
    MobilizeServer();
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = 2;
    server.ReceiveMessage(message.Serialize(), 2);
    for (auto instance: clusterConfiguration.instanceIds) {
        if (instance != serverConfiguration.selfInstanceId) {
            Raft::Message message;
            message.type = Raft::Message::Type::RequestVoteResults;
            message.requestVoteResults.term = 1;
            message.requestVoteResults.voteGranted = true;
            server.ReceiveMessage(message.Serialize(), instance);
        }
    }
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout - 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Follower,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, CandidateShouldRevertToFollowerWhenSameTermHeartbeatReceived) {
    // Arrange
    MobilizeServer();
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = 1;
    server.ReceiveMessage(message.Serialize(), 2);
    for (auto instance: clusterConfiguration.instanceIds) {
        if (instance != serverConfiguration.selfInstanceId) {
            Raft::Message message;
            message.type = Raft::Message::Type::RequestVoteResults;
            message.requestVoteResults.term = 1;
            message.requestVoteResults.voteGranted = true;
            server.ReceiveMessage(message.Serialize(), instance);
        }
    }
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout - 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Follower,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, LeaderShouldSendRegularHeartbeats) {
    // Arrange
    BecomeLeader();
    const auto expectedSerializedMessage = Json::Object({
        {"type", "AppendEntries"},
        {"term", 1},
        {"leaderCommit", 0},
        {"prevLogIndex", 0},
        {"prevLogTerm", 0},
        {"log", Json::Array({
        })},
    });

    // Act
    mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    std::map< int, size_t > heartbeatsReceivedPerInstance;
    for (auto instanceNumber: clusterConfiguration.instanceIds) {
        heartbeatsReceivedPerInstance[instanceNumber] = 0;
    }
    for (const auto& messageSent: messagesSent) {
        if (messageSent.message.type == Raft::Message::Type::AppendEntries) {
            ++heartbeatsReceivedPerInstance[messageSent.receiverInstanceNumber];
            const auto serializedMessage = messageSent.message.Serialize();
            EXPECT_EQ(
                expectedSerializedMessage,
                Json::Value::FromEncoding(serializedMessage)
            );
        }
    }
    for (auto instanceNumber: clusterConfiguration.instanceIds) {
        if (instanceNumber == serverConfiguration.selfInstanceId) {
            EXPECT_EQ(0, heartbeatsReceivedPerInstance[instanceNumber]);
        } else {
            EXPECT_NE(0, heartbeatsReceivedPerInstance[instanceNumber]);
        }
    }
}

TEST_F(ServerTests, RepeatVotesShouldNotCount) {
    // Arrange
    MobilizeServer();
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: clusterConfiguration.instanceIds) {
        if (instance != serverConfiguration.selfInstanceId) {
            Raft::Message message;
            message.type = Raft::Message::Type::RequestVoteResults;
            message.requestVoteResults.term = 1;
            if (instance == 2) {
                message.requestVoteResults.voteGranted = true;
            } else {
                message.requestVoteResults.voteGranted = false;
            }
            server.ReceiveMessage(message.Serialize(), instance);
        }
    }
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVoteResults;
    message.requestVoteResults.term = 1;
    message.requestVoteResults.voteGranted = true;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Candidate,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, UpdateTermWhenReceivingHeartBeat) {
    // Arrange
    MobilizeServer();

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = 2;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(2, mockPersistentState->variables.currentTerm);
}

TEST_F(ServerTests, ReceivingFirstHeartBeatAsFollowerSameTerm) {
    // Arrange
    bool leadershipChangeAnnounced = false;
    struct {
        int leaderId = 0;
        int term = 0;
    } leadershipChangeDetails;
    server.SetLeadershipChangeDelegate(
        [
            &leadershipChangeAnnounced,
            &leadershipChangeDetails
        ](
            int leaderId,
            int term
        ){
            leadershipChangeAnnounced = true;
            leadershipChangeDetails.leaderId = leaderId;
            leadershipChangeDetails.term = term;
        }
    );
    constexpr int leaderId = 2;
    constexpr int newTerm = 1;
    mockPersistentState->variables.currentTerm = newTerm;
    MobilizeServer();

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = newTerm;
    server.ReceiveMessage(message.Serialize(), leaderId);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_TRUE(leadershipChangeAnnounced);
    EXPECT_EQ(leaderId, leadershipChangeDetails.leaderId);
    EXPECT_EQ(newTerm, leadershipChangeDetails.term);
}

TEST_F(ServerTests, ReceivingSecondHeartBeatAsFollowerSameTerm) {
    // Arrange
    bool leadershipChangeAnnounced = false;
    struct {
        int leaderId = 0;
        int term = 0;
    } leadershipChangeDetails;
    server.SetLeadershipChangeDelegate(
        [
            &leadershipChangeAnnounced,
            &leadershipChangeDetails
        ](
            int leaderId,
            int term
        ){
            leadershipChangeAnnounced = true;
            leadershipChangeDetails.leaderId = leaderId;
            leadershipChangeDetails.term = term;
        }
    );
    constexpr int leaderId = 2;
    constexpr int newTerm = 1;
    mockPersistentState->variables.currentTerm = newTerm;
    MobilizeServer();
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = newTerm;
    server.ReceiveMessage(message.Serialize(), leaderId);
    server.WaitForAtLeastOneWorkerLoop();
    leadershipChangeAnnounced = false;

    // Act
    message = Raft::Message();
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = newTerm;
    server.ReceiveMessage(message.Serialize(), leaderId);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_FALSE(leadershipChangeAnnounced);
}

TEST_F(ServerTests, ReceivingFirstHeartBeatAsFollowerNewerTerm) {
    // Arrange
    bool leadershipChangeAnnounced = false;
    struct {
        int leaderId = 0;
        int term = 0;
    } leadershipChangeDetails;
    server.SetLeadershipChangeDelegate(
        [
            &leadershipChangeAnnounced,
            &leadershipChangeDetails
        ](
            int leaderId,
            int term
        ){
            leadershipChangeAnnounced = true;
            leadershipChangeDetails.leaderId = leaderId;
            leadershipChangeDetails.term = term;
        }
    );
    constexpr int leaderId = 2;
    constexpr int newTerm = 1;
    mockPersistentState->variables.currentTerm = 0;
    MobilizeServer();

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = newTerm;
    server.ReceiveMessage(message.Serialize(), leaderId);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_TRUE(leadershipChangeAnnounced);
    EXPECT_EQ(leaderId, leadershipChangeDetails.leaderId);
    EXPECT_EQ(newTerm, leadershipChangeDetails.term);
}

TEST_F(ServerTests, ReceivingSecondHeartBeatAsFollowerNewerTerm) {
    // Arrange
    bool leadershipChangeAnnounced = false;
    struct {
        int leaderId = 0;
        int term = 0;
    } leadershipChangeDetails;
    server.SetLeadershipChangeDelegate(
        [
            &leadershipChangeAnnounced,
            &leadershipChangeDetails
        ](
            int leaderId,
            int term
        ){
            leadershipChangeAnnounced = true;
            leadershipChangeDetails.leaderId = leaderId;
            leadershipChangeDetails.term = term;
        }
    );
    constexpr int leaderId = 2;
    constexpr int newTerm = 1;
    mockPersistentState->variables.currentTerm = 0;
    MobilizeServer();
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = newTerm;
    server.ReceiveMessage(message.Serialize(), leaderId);
    server.WaitForAtLeastOneWorkerLoop();
    leadershipChangeAnnounced = false;

    // Act
    message = Raft::Message();
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = newTerm;
    server.ReceiveMessage(message.Serialize(), leaderId);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_FALSE(leadershipChangeAnnounced);
}

TEST_F(ServerTests, ReceivingTwoHeartBeatAsFollowerSequentialTerms) {
    // Arrange
    bool leadershipChangeAnnounced = false;
    struct {
        int leaderId = 0;
        int term = 0;
    } leadershipChangeDetails;
    server.SetLeadershipChangeDelegate(
        [
            &leadershipChangeAnnounced,
            &leadershipChangeDetails
        ](
            int leaderId,
            int term
        ){
            leadershipChangeAnnounced = true;
            leadershipChangeDetails.leaderId = leaderId;
            leadershipChangeDetails.term = term;
        }
    );
    constexpr int firstLeaderId = 2;
    constexpr int secondLeaderId = 2;
    constexpr int firstTerm = 1;
    constexpr int secondTerm = 2;
    mockPersistentState->variables.currentTerm = 0;
    MobilizeServer();
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = firstTerm;
    server.ReceiveMessage(message.Serialize(), firstLeaderId);
    server.WaitForAtLeastOneWorkerLoop();
    leadershipChangeAnnounced = false;

    // Act
    message = Raft::Message();
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = secondTerm;
    server.ReceiveMessage(message.Serialize(), secondLeaderId);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_TRUE(leadershipChangeAnnounced);
    EXPECT_EQ(secondLeaderId, leadershipChangeDetails.leaderId);
    EXPECT_EQ(secondTerm, leadershipChangeDetails.term);
}

TEST_F(ServerTests, ReceivingHeartBeatFromSameTermShouldResetElectionTimeout) {
    // Arrange
    MobilizeServer();

    // Act
    while (mockTimeKeeper->currentTime <= serverConfiguration.maximumElectionTimeout * 2) {
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntries;
        message.appendEntries.term = 2;
        server.ReceiveMessage(message.Serialize(), 2);
        mockTimeKeeper->currentTime += 0.001;
        server.WaitForAtLeastOneWorkerLoop();
        EXPECT_EQ(
            Raft::Server::ElectionState::Follower,
            server.GetElectionState()
        );
    }

    // Assert
}

TEST_F(ServerTests, IgnoreHeartBeatFromOldTerm) {
    // Arrange
    MobilizeServer();
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = 2;
    server.ReceiveMessage(message.Serialize(), 2);

    // Act
    message = Raft::Message();
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = 1;
    server.ReceiveMessage(message.Serialize(), 2);

    // Assert
    EXPECT_EQ(2, mockPersistentState->variables.currentTerm);
}

TEST_F(ServerTests, ReceivingHeartBeatFromOldTermShouldNotResetElectionTimeout) {
    // Arrange
    mockPersistentState->variables.currentTerm = 42;
    MobilizeServer();

    // Act
    bool electionStarted = false;
    while (mockTimeKeeper->currentTime <= serverConfiguration.maximumElectionTimeout * 2) {
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntries;
        message.appendEntries.term = 13;
        server.ReceiveMessage(message.Serialize(), 2);
        mockTimeKeeper->currentTime += 0.001;
        server.WaitForAtLeastOneWorkerLoop();
        if (!messagesSent.empty()) {
            electionStarted = true;
            break;
        }
    }

    // Assert
    ASSERT_TRUE(electionStarted);
}

TEST_F(ServerTests, CandidateShouldRevertToFollowerWhenGreaterTermRequestVoteReceived) {
    // Arrange
    BecomeCandidate();

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 3;
    message.requestVote.candidateId = 2;
    message.requestVote.lastLogTerm = 999;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();
    mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout - 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(0, messagesSent.size());
    EXPECT_EQ(
        Raft::IServer::ElectionState::Follower,
        server.GetElectionState()
    );
    EXPECT_EQ(3, mockPersistentState->variables.currentTerm);
}

TEST_F(ServerTests, CandidateShouldRevertToFollowerWhenGreaterTermRequestVoteResultsReceived) {
    // Arrange
    BecomeCandidate(1);

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVoteResults;
    message.requestVoteResults.term = 3;
    message.requestVoteResults.voteGranted = false;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Follower,
        server.GetElectionState()
    );
    EXPECT_EQ(3, mockPersistentState->variables.currentTerm);
}

TEST_F(ServerTests, LeadershipGainAnnouncement) {
    // Arrange
    bool leadershipChangeAnnounced = false;
    struct {
        int leaderId = 0;
        int term = 0;
    } leadershipChangeDetails;
    server.SetLeadershipChangeDelegate(
        [
            &leadershipChangeAnnounced,
            &leadershipChangeDetails
        ](
            int leaderId,
            int term
        ){
            leadershipChangeAnnounced = true;
            leadershipChangeDetails.leaderId = leaderId;
            leadershipChangeDetails.term = term;
        }
    );

    // Act
    mockPersistentState->variables.currentTerm = 0;
    serverConfiguration.selfInstanceId = 5;
    BecomeLeader();

    // Assert
    ASSERT_TRUE(leadershipChangeAnnounced);
    EXPECT_EQ(5, leadershipChangeDetails.leaderId);
    EXPECT_EQ(1, leadershipChangeDetails.term);
}

TEST_F(ServerTests, NoLeadershipGainWhenNotYetLeader) {
    // Arrange
    bool leadershipChangeAnnounced = false;
    server.SetLeadershipChangeDelegate(
        [
            &leadershipChangeAnnounced
        ](
            int leaderId,
            int term
        ){
            leadershipChangeAnnounced = true;
        }
    );

    // Act
    mockPersistentState->variables.currentTerm = 0;
    serverConfiguration.selfInstanceId = 5;
    MobilizeServer();
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    auto instanceNumbersEntry = clusterConfiguration.instanceIds.begin();
    size_t votesGranted = 0;
    do {
        const auto instance = *instanceNumbersEntry++;
        if (instance == serverConfiguration.selfInstanceId) {
            continue;
        }
        Raft::Message message;
        message.type = Raft::Message::Type::RequestVoteResults;
        message.requestVoteResults.term = 1;
        message.requestVoteResults.voteGranted = true;
        server.ReceiveMessage(message.Serialize(), instance);
        server.WaitForAtLeastOneWorkerLoop();
        EXPECT_FALSE(leadershipChangeAnnounced) << votesGranted;
        ++votesGranted;
    } while (votesGranted + 1 < clusterConfiguration.instanceIds.size() / 2);

    // Assert
}

TEST_F(ServerTests, NoLeadershipGainWhenAlreadyLeader) {
    // Arrange
    bool leadershipChangeAnnounced = false;
    server.SetLeadershipChangeDelegate(
        [
            &leadershipChangeAnnounced
        ](
            int leaderId,
            int term
        ){
            leadershipChangeAnnounced = true;
        }
    );

    // Act
    mockPersistentState->variables.currentTerm = 0;
    serverConfiguration.selfInstanceId = 5;
    MobilizeServer();
    mockTimeKeeper->currentTime = serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: clusterConfiguration.instanceIds) {
        if (instance == serverConfiguration.selfInstanceId) {
            continue;
        }
        const auto wasLeader = (
            server.GetElectionState() == Raft::IServer::ElectionState::Leader
        );
        Raft::Message message;
        message.type = Raft::Message::Type::RequestVoteResults;
        message.requestVoteResults.term = 1;
        message.requestVoteResults.voteGranted = true;
        server.ReceiveMessage(message.Serialize(), instance);
        server.WaitForAtLeastOneWorkerLoop();
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

TEST_F(ServerTests, AnnounceLeaderWhenAFollower) {
    // Arrange
    bool leadershipChangeAnnounced = false;
    struct {
        int leaderId = 0;
        int term = 0;
    } leadershipChangeDetails;
    server.SetLeadershipChangeDelegate(
        [
            &leadershipChangeAnnounced,
            &leadershipChangeDetails
        ](
            int leaderId,
            int term
        ){
            leadershipChangeAnnounced = true;
            leadershipChangeDetails.leaderId = leaderId;
            leadershipChangeDetails.term = term;
        }
    );
    constexpr int leaderId = 2;
    constexpr int newTerm = 1;

    // Act
    mockPersistentState->variables.currentTerm = 0;
    serverConfiguration.selfInstanceId = 5;
    BecomeFollower(leaderId, newTerm);

    // Assert
    ASSERT_TRUE(leadershipChangeAnnounced);
    EXPECT_EQ(leaderId, leadershipChangeDetails.leaderId);
    EXPECT_EQ(newTerm, leadershipChangeDetails.term);
}

TEST_F(ServerTests, LeaderAppendLogEntry) {
    // Arrange
    Raft::LogEntry committedEntry;
    committedEntry.term = 1;
    mockLog->entries.push_back(std::move(committedEntry));
    BecomeLeader(3);
    server.SetCommitIndex(1);
    std::vector< Raft::LogEntry > entries;
    Raft::LogEntry firstEntry;
    firstEntry.term = 2;
    entries.push_back(std::move(firstEntry));
    Raft::LogEntry secondEntry;
    secondEntry.term = 3;
    entries.push_back(std::move(secondEntry));
    const auto expectedSerializedMessage = Json::Object({
        {"type", "AppendEntries"},
        {"term", 3},
        {"leaderCommit", 1},
        {"prevLogIndex", 1},
        {"prevLogTerm", 1},
        {"log", Json::Array({
            Json::Object({
                {"term", 2},
            }),
            Json::Object({
                {"term", 3},
            }),
        })},
    });

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
            const auto serializedMessage = messageSent.message.Serialize();
            EXPECT_EQ(
                expectedSerializedMessage,
                Json::Value::FromEncoding(serializedMessage)
            );
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

TEST_F(ServerTests, FollowerAppendLogEntry) {
    // Arrange
    constexpr int leaderId = 2;
    constexpr int newTerm = 9;
    mockPersistentState->variables.currentTerm = 0;
    serverConfiguration.selfInstanceId = 5;
    BecomeFollower(leaderId, newTerm);
    std::vector< Raft::LogEntry > entries;
    Raft::LogEntry firstEntry;
    firstEntry.term = 4;
    entries.push_back(std::move(firstEntry));
    Raft::LogEntry secondEntry;
    secondEntry.term = 5;
    entries.push_back(std::move(secondEntry));

    // Act
    EXPECT_EQ(0, server.GetLastIndex());
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = 9;
    message.appendEntries.prevLogIndex = 0;
    message.appendEntries.prevLogTerm = 0;
    message.log = entries;
    server.ReceiveMessage(message.Serialize(), leaderId);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(2, server.GetLastIndex());
    ASSERT_EQ(entries.size(), mockLog->entries.size());
    for (size_t i = 0; i < entries.size(); ++i) {
        EXPECT_EQ(entries[i].term, mockLog->entries[i].term);
    }
}

TEST_F(ServerTests, LeaderDoNotAdvanceCommitIndexWhenMajorityOfClusterHasNotYetAppliedLogEntry) {
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

TEST_F(ServerTests, LeaderAdvanceCommitIndexWhenMajorityOfClusterHasAppliedLogEntry) {
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
            Raft::Message message;
            message.type = Raft::Message::Type::AppendEntriesResults;
            message.appendEntriesResults.term = 7;
            message.appendEntriesResults.success = true;
            message.appendEntriesResults.matchIndex = 1;
            server.ReceiveMessage(message.Serialize(), instance);
            server.WaitForAtLeastOneWorkerLoop();
            EXPECT_EQ(0, server.GetCommitIndex());
        }
    }
    server.AppendLogEntries({secondEntry});
    server.WaitForAtLeastOneWorkerLoop();
    size_t successfulResponseCount = 0;
    size_t responseCount = 0;
    for (auto instance: clusterConfiguration.instanceIds) {
        if (instance != serverConfiguration.selfInstanceId) {
            Raft::Message message;
            message.type = Raft::Message::Type::AppendEntriesResults;
            message.appendEntriesResults.term = 7;
            if (instance == 2) {
                message.appendEntriesResults.success = false;
                message.appendEntriesResults.matchIndex = 1;
            } else {
                ++successfulResponseCount;
                message.appendEntriesResults.success = true;
                message.appendEntriesResults.matchIndex = 2;
            }
            ++responseCount;
            server.ReceiveMessage(message.Serialize(), instance);
            server.WaitForAtLeastOneWorkerLoop();
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

TEST_F(ServerTests, FollowerAdvanceCommitIndexWhenMajorityOfClusterHasAppliedLogEntry) {
    // Arrange
    constexpr int leaderId = 2;
    constexpr int newTerm = 9;
    mockPersistentState->variables.currentTerm = 0;
    serverConfiguration.selfInstanceId = 5;
    BecomeFollower(leaderId, newTerm);

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = 9;
    message.appendEntries.leaderCommit = 0;
    Raft::LogEntry firstEntry;
    firstEntry.term = 4;
    message.log.push_back(std::move(firstEntry));
    server.ReceiveMessage(message.Serialize(), leaderId);
    server.WaitForAtLeastOneWorkerLoop();
    EXPECT_EQ(0, server.GetCommitIndex());
    message.appendEntries.leaderCommit = 1;
    message.log.clear();
    Raft::LogEntry secondEntry;
    secondEntry.term = 5;
    message.log.push_back(std::move(secondEntry));
    server.ReceiveMessage(message.Serialize(), leaderId);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(1, server.GetCommitIndex());
    EXPECT_EQ(1, mockLog->commitIndex);
}

TEST_F(ServerTests, AppendEntriesWhenNotLeader) {
    // Arrange
    constexpr int leaderId = 2;
    constexpr int newTerm = 1;
    mockPersistentState->variables.currentTerm = 0;
    serverConfiguration.selfInstanceId = 5;
    BecomeFollower(leaderId, newTerm);
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

TEST_F(ServerTests, InitializeLastIndex) {
    // Arrange
    Raft::LogEntry firstEntry;
    firstEntry.term = 1;
    mockLog->entries.push_back(std::move(firstEntry));
    Raft::LogEntry secondEntry;
    secondEntry.term = 1;
    mockLog->entries.push_back(std::move(secondEntry));

    // Act
    MobilizeServer();

    // Assert
    EXPECT_EQ(2, server.GetLastIndex());
}

TEST_F(ServerTests, LeaderInitialAppendEntriesFromEndOfLog) {
    // Arrange
    Raft::LogEntry firstEntry;
    firstEntry.term = 3;
    mockLog->entries.push_back(std::move(firstEntry));
    Raft::LogEntry secondEntry;
    secondEntry.term = 7;
    mockLog->entries.push_back(std::move(secondEntry));
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

TEST_F(ServerTests, LeaderAppendOlderEntriesAfterDiscoveringFollowerIsBehind) {
    // Arrange
    Raft::LogEntry firstEntry;
    firstEntry.term = 3;
    mockLog->entries.push_back(std::move(firstEntry));
    Raft::LogEntry secondEntry;
    secondEntry.term = 7;
    mockLog->entries.push_back(std::move(secondEntry));
    BecomeLeader(8);
    mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    messagesSent.clear();
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntriesResults;
    message.appendEntriesResults.term = 8;
    message.appendEntriesResults.success = false;
    message.appendEntriesResults.matchIndex = 1;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(Raft::Message::Type::AppendEntries, messagesSent[0].message.type);
    EXPECT_EQ(1, messagesSent[0].message.appendEntries.prevLogIndex);
    EXPECT_EQ(3, messagesSent[0].message.appendEntries.prevLogTerm);
    ASSERT_EQ(1, messagesSent[0].message.log.size());
    EXPECT_EQ(secondEntry.term, messagesSent[0].message.log[0].term);
}

TEST_F(ServerTests, AppendEntriesNotSentIfLastNotYetAcknowledged) {
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

TEST_F(ServerTests, NextIndexAdvancedAndNextEntryAppendedAfterPreviousAcknowledged) {
    // Arrange
    Raft::LogEntry firstEntry, secondEntry;
    firstEntry.term = 2;
    secondEntry.term = 3;
    mockLog->entries.push_back(std::move(firstEntry));
    BecomeLeader(8);
    mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
    server.WaitForAtLeastOneWorkerLoop();
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntriesResults;
    message.appendEntriesResults.term = 8;
    message.appendEntriesResults.success = false;
    message.appendEntriesResults.matchIndex = 0;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    messagesSent.clear();
    server.AppendLogEntries({secondEntry});
    server.WaitForAtLeastOneWorkerLoop();
    message = Raft::Message();
    message.type = Raft::Message::Type::AppendEntriesResults;
    message.appendEntriesResults.term = 8;
    message.appendEntriesResults.success = true;
    message.appendEntriesResults.matchIndex = 1;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();

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
            EXPECT_EQ(firstEntry.term, messageSent.message.appendEntries.prevLogTerm);
            ASSERT_EQ(1, messageSent.message.log.size());
            EXPECT_EQ(secondEntry.term, messageSent.message.log[0].term);
        }
    }
    EXPECT_TRUE(sendEntrySent);
}

TEST_F(ServerTests, NoHeartBeatShouldBeSentWhilePreviousAppendEntriesUnacknowledged) {
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

TEST_F(ServerTests, IgnoreRequestVoteResultsIfFollower) {
    // Arrange
    mockPersistentState->variables.currentTerm = 1;
    MobilizeServer();
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVoteResults;
    message.requestVoteResults.term = 1;
    message.requestVoteResults.voteGranted = true;

    // Act
    for (auto instance: clusterConfiguration.instanceIds) {
        if (instance != serverConfiguration.selfInstanceId) {
            server.ReceiveMessage(message.Serialize(), instance);
        }
    }
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Follower,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, IgnoreAppendEntriesResultsIfNotLeader) {
    // Arrange
    mockPersistentState->variables.currentTerm = 1;
    MobilizeServer();
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntriesResults;
    message.appendEntriesResults.term = 1;
    message.appendEntriesResults.success = true;
    message.appendEntriesResults.matchIndex = 42;

    // Act
    for (auto instance: clusterConfiguration.instanceIds) {
        if (instance != serverConfiguration.selfInstanceId) {
            server.ReceiveMessage(message.Serialize(), instance);
        }
    }
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    for (const auto& messageSent: messagesSent) {
        EXPECT_NE(Raft::Message::Type::AppendEntries, messageSent.message.type);
    }
    EXPECT_EQ(0, server.GetCommitIndex());
}

TEST_F(ServerTests, IgnoreStaleYesVoteFromPreviousTerm) {
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

TEST_F(ServerTests, IgnoreStaleNoVoteFromPreviousTerm) {
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

TEST_F(ServerTests, RetransmitUnacknowledgedAppendEntries) {
    // Arrange
    Raft::LogEntry testEntry;
    testEntry.term = 3;
    BecomeLeader(3);

    // Act
    server.AppendLogEntries({testEntry});
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();
    mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    std::map< int, bool > appendEntriesReceivedPerInstance;
    for (auto instanceNumber: clusterConfiguration.instanceIds) {
        appendEntriesReceivedPerInstance[instanceNumber] = false;
    }
    for (const auto& messageSent: messagesSent) {
        if (messageSent.message.type == Raft::Message::Type::AppendEntries) {
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

TEST_F(ServerTests, IgnoreDuplicateAppendEntriesResults) {
    // Arrange
    Raft::LogEntry testEntry;
    testEntry.term = 3;
    BecomeLeader(3);
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntriesResults;
    message.appendEntriesResults.term = 3;
    message.appendEntriesResults.success = true;
    message.appendEntriesResults.matchIndex = 1;

    // Act
    server.AppendLogEntries({testEntry});
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();
    mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
    server.WaitForAtLeastOneWorkerLoop();
    server.ReceiveMessage(message.Serialize(), 2);
    server.ReceiveMessage(message.Serialize(), 2);

    // Assert
    EXPECT_EQ(1, server.GetMatchIndex(2));
}

TEST_F(ServerTests, ReinitializeVolatileLeaderStateAfterElection) {
    // Arrange
    BecomeLeader(7);
    Raft::LogEntry testEntry;
    testEntry.term = 7;
    server.AppendLogEntries({testEntry});
    server.WaitForAtLeastOneWorkerLoop();
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntriesResults;
    message.appendEntriesResults.term = 7;
    message.appendEntriesResults.success = true;
    message.appendEntriesResults.matchIndex = 1;
    for (auto instance: clusterConfiguration.instanceIds) {
        if (instance != serverConfiguration.selfInstanceId) {
            server.ReceiveMessage(message.Serialize(), instance);
        }
    }
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    message = Raft::Message();
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = 8;
    message.appendEntries.leaderCommit = 1;
    message.appendEntries.prevLogIndex = 1;
    message.appendEntries.prevLogTerm = 7;
    server.ReceiveMessage(message.Serialize(), 2);
    mockTimeKeeper->currentTime += serverConfiguration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: clusterConfiguration.instanceIds) {
        if (instance != serverConfiguration.selfInstanceId) {
            Raft::Message message;
            message.type = Raft::Message::Type::RequestVoteResults;
            message.requestVoteResults.term = 9;
            message.requestVoteResults.voteGranted = true;
            server.ReceiveMessage(message.Serialize(), instance);
        }
    }
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();

    // Assert
    for (auto instance: clusterConfiguration.instanceIds) {
        if (instance != serverConfiguration.selfInstanceId) {
            EXPECT_EQ(2, server.GetNextIndex(instance));
            EXPECT_EQ(0, server.GetMatchIndex(instance));
        }
    }
}

TEST_F(ServerTests, FollowerReceiveAppendEntriesSuccess) {
    // Arrange
    Raft::LogEntry oldConflictingEntry, newConflictingEntry, nextEntry;
    oldConflictingEntry.term = 6;
    newConflictingEntry.term = 7;
    nextEntry.term = 8;
    mockLog->entries = {oldConflictingEntry};
    BecomeFollower(2, 8);

    // Act
    messagesSent.clear();
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = 8;
    message.appendEntries.leaderCommit = 1;
    message.appendEntries.prevLogIndex = 0;
    message.appendEntries.prevLogTerm = 0;
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
    EXPECT_EQ(8, messagesSent[0].message.appendEntriesResults.term);
    EXPECT_TRUE(messagesSent[0].message.appendEntriesResults.success);
    EXPECT_EQ(2, messagesSent[0].message.appendEntriesResults.matchIndex);
}

TEST_F(ServerTests, FollowerReceiveAppendEntriesFailureOldTerm) {
    // Arrange
    Raft::LogEntry oldConflictingEntry, newConflictingEntry, nextEntry;
    oldConflictingEntry.term = 6;
    newConflictingEntry.term = 7;
    nextEntry.term = 8;
    mockLog->entries = {oldConflictingEntry};
    BecomeFollower(2, 8);

    // Act
    messagesSent.clear();
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = 8;
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
    EXPECT_EQ(8, messagesSent[0].message.appendEntriesResults.term);
    EXPECT_FALSE(messagesSent[0].message.appendEntriesResults.success);
    EXPECT_EQ(0, messagesSent[0].message.appendEntriesResults.matchIndex);
}

TEST_F(ServerTests, FollowerReceiveAppendEntriesFailurePreviousNotFound) {
    // Arrange
    Raft::LogEntry oldConflictingEntry, newConflictingEntry, nextEntry;
    oldConflictingEntry.term = 6;
    newConflictingEntry.term = 7;
    nextEntry.term = 8;
    mockLog->entries = {};
    BecomeFollower(2, 8);

    // Act
    messagesSent.clear();
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = 8;
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
    EXPECT_EQ(8, messagesSent[0].message.appendEntriesResults.term);
    EXPECT_FALSE(messagesSent[0].message.appendEntriesResults.success);
    EXPECT_EQ(0, messagesSent[0].message.appendEntriesResults.matchIndex);
}

TEST_F(ServerTests, PersistentStateSavedWhenVoteIsCastAsFollower) {
    // Arrange
    MobilizeServer();
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 1;
    message.requestVote.candidateId = 2;
    message.requestVote.lastLogTerm = 999;

    // Act
    server.ReceiveMessage(message.Serialize(), 2);

    // Assert
    EXPECT_EQ(2, mockPersistentState->variables.votedFor);
    EXPECT_TRUE(mockPersistentState->variables.votedThisTerm);
}

TEST_F(ServerTests, PersistentStateSavedWhenVoteIsCastAsCandidate) {
    // Arrange

    // Act
    BecomeCandidate(4);

    // Assert
    EXPECT_TRUE(mockPersistentState->variables.votedThisTerm);
    EXPECT_EQ(serverConfiguration.selfInstanceId, mockPersistentState->variables.votedFor);
    EXPECT_EQ(4, mockPersistentState->variables.currentTerm);
}

TEST_F(ServerTests, CrashedFollowerRestartsAndRepeatsVoteResults) {
    // Arrange
    MobilizeServer();
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 1;
    message.requestVote.candidateId = 2;
    message.requestVote.lastLogTerm = 999;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();
    server.Demobilize();
    server = Raft::Server();
    SetServerDelegates();
    MobilizeServer();

    // Act
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(
        Raft::Message::Type::RequestVoteResults,
        messagesSent[0].message.type
    );
    EXPECT_EQ(1, messagesSent[0].message.requestVoteResults.term);
    EXPECT_TRUE(messagesSent[0].message.requestVoteResults.voteGranted);
}

TEST_F(ServerTests, CrashedFollowerRestartsAndRejectsVoteFromDifferentCandidate) {
    // Arrange
    MobilizeServer();
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 1;
    message.requestVote.candidateId = 2;
    message.requestVote.lastLogTerm = 999;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();
    server.Demobilize();
    server = Raft::Server();
    SetServerDelegates();
    MobilizeServer();

    // Act
    message.requestVote.candidateId = 10;
    server.ReceiveMessage(message.Serialize(), 10);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(
        Raft::Message::Type::RequestVoteResults,
        messagesSent[0].message.type
    );
    EXPECT_EQ(1, messagesSent[0].message.requestVoteResults.term);
    EXPECT_FALSE(messagesSent[0].message.requestVoteResults.voteGranted);
}

TEST_F(ServerTests, ConfigurationUpdateForNewTermWhenReceivingVoteRequestForNewCandidate) {
    // Arrange
    mockPersistentState->variables.currentTerm = 4;
    MobilizeServer();

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVote;
    message.requestVote.term = 5;
    message.requestVote.candidateId = 2;
    message.requestVote.lastLogTerm = 999;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(5, mockPersistentState->variables.currentTerm);
}

TEST_F(ServerTests, ConfigurationUpdateForNewTermWhenVoteRejectedByNewerTermServer) {
    // Arrange
    BecomeCandidate(4);

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::RequestVoteResults;
    message.requestVoteResults.term = 5;
    message.requestVoteResults.voteGranted = false;
    server.ReceiveMessage(message.Serialize(), 2);

    // Assert
    EXPECT_EQ(5, mockPersistentState->variables.currentTerm);
}

TEST_F(ServerTests, ConfigurationUpdateForNewTermWhenReceiveAppendEntriesFromNewerTermLeader) {
    // Arrange
    mockPersistentState->variables.currentTerm = 4;
    MobilizeServer();

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntries;
    message.appendEntries.term = 5;
    message.appendEntries.leaderCommit = 95;
    message.appendEntries.prevLogIndex = 0;
    message.appendEntries.prevLogTerm = 0;
    server.ReceiveMessage(message.Serialize(), 2);

    // Assert
    EXPECT_EQ(5, mockPersistentState->variables.currentTerm);
}
