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
#include <Raft/LogEntry.hpp>
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
        size_t commitCount = 0;
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
            ++commitCount;
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
        size_t saveCount = 0;

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
            ++saveCount;
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
    Raft::ClusterConfiguration clusterConfiguration;
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

    void AdvanceTimeToJustBeforeElectionTimeout() {
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout - 0.001;
        server.WaitForAtLeastOneWorkerLoop();
    }

    void AppendNoOpEntry(int term) {
        Raft::LogEntry entry;
        entry.term = term;
        mockLog->entries.push_back(std::move(entry));
    }

    void CastVote(
        int instance,
        int term,
        bool granted = true
    ) {
        if (instance != serverConfiguration.selfInstanceId) {
            Raft::Message message;
            message.type = Raft::Message::Type::RequestVoteResults;
            message.requestVoteResults.term = term;
            message.requestVoteResults.voteGranted = granted;
            server.ReceiveMessage(message.Serialize(), instance);
            server.WaitForAtLeastOneWorkerLoop();
        }
    }

    void CastVotes(
        int term,
        bool granted = true
    ) {
        for (auto instance: clusterConfiguration.instanceIds) {
            CastVote(instance, term, granted);
        }
    }

    void RequestVote(
        int instance,
        int term,
        int lastLogIndex
    ) {
        int lastLogTerm = 0;
        if (lastLogIndex != 0) {
            lastLogTerm = mockLog->entries[lastLogIndex - 1].term;
        }
        RequestVote(instance, term, lastLogIndex, lastLogTerm);
    }

    void RequestVote(
        int instance,
        int term,
        int lastLogIndex,
        int lastLogTerm
    ) {
        Raft::Message message;
        message.type = Raft::Message::Type::RequestVote;
        message.requestVote.term = term;
        message.requestVote.candidateId = instance;
        message.requestVote.lastLogTerm = lastLogTerm;
        message.requestVote.lastLogIndex = lastLogIndex;
        server.ReceiveMessage(message.Serialize(), instance);
        server.WaitForAtLeastOneWorkerLoop();
    }

    void ReceiveAppendEntriesFromMockLeader(
        int leaderId,
        int term,
        size_t leaderCommit
    ) {
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntries;
        message.appendEntries.term = term;
        message.appendEntries.leaderCommit = leaderCommit;
        if (mockLog->entries.empty()) {
            message.appendEntries.prevLogIndex = 0;
            message.appendEntries.prevLogTerm = 0;
        } else {
            message.appendEntries.prevLogIndex = mockLog->entries.size();
            message.appendEntries.prevLogTerm = term;
        }
        server.ReceiveMessage(message.Serialize(), leaderId);
        server.WaitForAtLeastOneWorkerLoop();
        messagesSent.clear();
    }

    void ReceiveAppendEntriesFromMockLeader(
        int leaderId,
        int term
    ) {
        ReceiveAppendEntriesFromMockLeader(
            leaderId,
            term,
            mockLog->entries.size()
        );
    }

    void ReceiveAppendEntriesFromMockLeader(
        int leaderId,
        int term,
        size_t leaderCommit,
        size_t prevLogIndex,
        const std::vector< Raft::LogEntry >& entries,
        bool clearMessagesSent = true
    ) {
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntries;
        message.appendEntries.term = term;
        message.appendEntries.leaderCommit = leaderCommit;
        message.appendEntries.prevLogIndex = prevLogIndex;
        if (prevLogIndex == 0) {
            message.appendEntries.prevLogTerm = 0;
        } else {
            message.appendEntries.prevLogTerm = mockLog->entries[prevLogIndex - 1].term;
        }
        message.log = entries;
        server.ReceiveMessage(message.Serialize(), leaderId);
        server.WaitForAtLeastOneWorkerLoop();
        if (clearMessagesSent) {
            messagesSent.clear();
        }
    }

    void ReceiveAppendEntriesFromMockLeader(
        int leaderId,
        int term,
        size_t leaderCommit,
        const std::vector< Raft::LogEntry >& entries,
        bool clearMessagesSent = true
    ) {
        ReceiveAppendEntriesFromMockLeader(
            leaderId,
            term,
            leaderCommit,
            mockLog->entries.size(),
            entries,
            clearMessagesSent
        );
    }

    void ReceiveAppendEntriesFromMockLeader(
        int leaderId,
        int term,
        const std::vector< Raft::LogEntry >& entries
    ) {
        ReceiveAppendEntriesFromMockLeader(
            leaderId,
            term,
            0,
            entries
        );
    }

    void WaitForElectionTimeout() {
        mockTimeKeeper->currentTime += serverConfiguration.maximumElectionTimeout;
        server.WaitForAtLeastOneWorkerLoop();
    }

    void BecomeLeader(
        int term = 1,
        bool acknowledgeInitialHeartbeats = true
    ) {
        mockPersistentState->variables.currentTerm = term - 1;
        MobilizeServer();
        WaitForElectionTimeout();
        CastVotes(term);
        if (acknowledgeInitialHeartbeats) {
            for (auto instance: clusterConfiguration.instanceIds) {
                if (instance != serverConfiguration.selfInstanceId) {
                    Raft::Message message;
                    message.type = Raft::Message::Type::AppendEntriesResults;
                    message.appendEntriesResults.term = term;
                    message.appendEntriesResults.success = true;
                    message.appendEntriesResults.matchIndex = mockLog->entries.size();
                    server.ReceiveMessage(message.Serialize(), instance);
                }
            }
            messagesSent.clear();
        }
    }

    void BecomeCandidate(int term = 1) {
        mockPersistentState->variables.currentTerm = term - 1;
        MobilizeServer();
        WaitForElectionTimeout();
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
    AdvanceTimeToJustBeforeElectionTimeout();

    // Assert
    EXPECT_EQ(0, messagesSent.size());
}

TEST_F(ServerTests, ElectionAlwaysStartedWithinMaximumTimeoutInterval) {
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

TEST_F(ServerTests, RequestVoteSentToAllServersExceptSelf) {
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

TEST_F(ServerTests, RequestVoteIncludesLastIndex) {
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

TEST_F(ServerTests, RequestVoteIncludesLastTermWithLog) {
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

TEST_F(ServerTests, RequestVoteIncludesLastTermWithoutLog) {
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

TEST_F(ServerTests, ServerVotesForItselfInElectionItStarts) {
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

TEST_F(ServerTests, ServerIncrementsTermInElectionItStarts) {
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

TEST_F(ServerTests, ServerDoesReceiveUnanimousVoteInElection) {
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

TEST_F(ServerTests, ServerDoesReceiveNonUnanimousMajorityVoteInElection) {
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

TEST_F(ServerTests, ServerRetransmitsRequestVoteForSlowVotersInElection) {
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

TEST_F(ServerTests, ServerDoesNotRetransmitTooQuickly) {
    // Arrange
    MobilizeServer();
    WaitForElectionTimeout();
    for (auto instance: clusterConfiguration.instanceIds) {
        switch (instance) {
            case 6:
            case 7:
            case 11: {
                CastVote(instance, 1, instance != 11);
            }
        }
    }

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
        ASSERT_EQ(
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
    WaitForElectionTimeout();

    // Act
    CastVotes(1, false);

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Candidate,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, ServerAlmostWinsElection) {
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

TEST_F(ServerTests, TimeoutBeforeMajorityVoteOrNewLeaderHeartbeat) {
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

TEST_F(ServerTests, ReceiveVoteRequestWhenSameTermNoVotePending) {
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

TEST_F(ServerTests, ReceiveVoteRequestWhenOurLogIsGreaterTerm) {
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

TEST_F(ServerTests, ReceiveVoteRequestWhenOurLogIsSameTermGreaterIndex) {
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

TEST_F(ServerTests, ReceiveVoteRequestWhenSameTermAlreadyVotedForAnother) {
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

TEST_F(ServerTests, ReceiveVoteRequestWhenSameTermAlreadyVotedForSame) {
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

TEST_F(ServerTests, GrantVoteRequestLesserTerm) {
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

TEST_F(ServerTests, GrantVoteRequestGreaterTermWhenFollower) {
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

TEST_F(ServerTests, DoNotVoteButUpdateTermFromVoteRequestGreaterTermWhenNonVotingMember) {
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

TEST_F(ServerTests, RejectVoteRequestFromCandidateWithOlderLog) {
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

TEST_F(ServerTests, GrantVoteRequestGreaterTermWhenCandidate) {
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

TEST_F(ServerTests, RejectVoteRequestGreaterTermWhenCandidate) {
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

TEST_F(ServerTests, GrantVoteRequestGreaterTermWhenLeader) {
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

TEST_F(ServerTests, RejectVoteRequestGreaterTermWhenLeader) {
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

TEST_F(ServerTests, DoNotStartVoteWhenAlreadyLeader) {
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

TEST_F(ServerTests, AfterRevertToFollowerDoNotStartNewElectionBeforeMinimumTimeout) {
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

TEST_F(ServerTests, LeaderShouldRevertToFollowerWhenGreaterTermHeartbeatReceived) {
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

TEST_F(ServerTests, CandidateShouldRevertToFollowerWhenGreaterTermHeartbeatReceived) {
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

TEST_F(ServerTests, CandidateShouldRevertToFollowerWhenSameTermHeartbeatReceived) {
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

TEST_F(ServerTests, CandidateShouldRevertToFollowerWhenGreaterTermVoteGrantReceived) {
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

TEST_F(ServerTests, CandidateShouldRevertToFollowerWhenGreaterTermVoteRejectReceived) {
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

TEST_F(ServerTests, LeaderShouldSendRegularHeartbeats) {
    // Arrange
    BecomeLeader();

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

TEST_F(ServerTests, UpdateTermWhenReceivingHeartBeat) {
    // Arrange
    MobilizeServer();

    // Act
    ReceiveAppendEntriesFromMockLeader(2, 2);

    // Assert
    EXPECT_EQ(2, mockPersistentState->variables.currentTerm);
}

TEST_F(ServerTests, ReceivingFirstHeartBeatAsFollowerSameTerm) {
    // Arrange
    constexpr int leaderId = 2;
    constexpr int newTerm = 1;
    mockPersistentState->variables.currentTerm = newTerm;
    MobilizeServer();

    // Act
    ReceiveAppendEntriesFromMockLeader(leaderId, newTerm);

    // Assert
    EXPECT_EQ(leaderId, server.GetClusterLeaderId());
    EXPECT_EQ(newTerm, mockPersistentState->variables.currentTerm);
}

TEST_F(ServerTests, ReceivingFirstHeartBeatAsFollowerNewerTerm) {
    // Arrange
    constexpr int leaderId = 2;
    constexpr int newTerm = 1;
    mockPersistentState->variables.currentTerm = 0;
    MobilizeServer();

    // Act
    ReceiveAppendEntriesFromMockLeader(leaderId, newTerm);

    // Assert
    EXPECT_EQ(leaderId, server.GetClusterLeaderId());
    EXPECT_EQ(newTerm, mockPersistentState->variables.currentTerm);
}

TEST_F(ServerTests, ReceivingTwoHeartBeatAsFollowerSequentialTerms) {
    // Arrange
    constexpr int firstLeaderId = 2;
    constexpr int secondLeaderId = 2;
    constexpr int firstTerm = 1;
    constexpr int secondTerm = 2;
    mockPersistentState->variables.currentTerm = 0;
    MobilizeServer();
    ReceiveAppendEntriesFromMockLeader(firstLeaderId, firstTerm);

    // Act
    ReceiveAppendEntriesFromMockLeader(secondLeaderId, secondTerm);

    // Assert
    EXPECT_EQ(secondLeaderId, server.GetClusterLeaderId());
    EXPECT_EQ(secondTerm, mockPersistentState->variables.currentTerm);
}

TEST_F(ServerTests, ReceivingHeartBeatFromSameTermShouldResetElectionTimeout) {
    // Arrange
    MobilizeServer();

    // Act
    while (mockTimeKeeper->currentTime <= serverConfiguration.maximumElectionTimeout * 2) {
        ReceiveAppendEntriesFromMockLeader(2, 2);
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2;
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
    ReceiveAppendEntriesFromMockLeader(2, 2);

    // Act
    ReceiveAppendEntriesFromMockLeader(2, 1);

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
        ReceiveAppendEntriesFromMockLeader(2, 13);
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2;
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

TEST_F(ServerTests, CandidateShouldRevertToFollowerWhenGreaterTermRequestVoteResultsReceived) {
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

TEST_F(ServerTests, LeaderAppendLogEntry) {
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

TEST_F(ServerTests, AppendEntriesWhenNotLeader) {
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

TEST_F(ServerTests, InitializeLastIndex) {
    // Arrange
    AppendNoOpEntry(1);
    AppendNoOpEntry(1);

    // Act
    MobilizeServer();

    // Assert
    EXPECT_EQ(2, server.GetLastIndex());
}

TEST_F(ServerTests, LeaderInitialAppendEntriesFromEndOfLog) {
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

TEST_F(ServerTests, LeaderAppendOlderEntriesAfterDiscoveringFollowerIsBehind) {
    // Arrange
    AppendNoOpEntry(3);
    AppendNoOpEntry(7);
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
    EXPECT_EQ(7, messagesSent[0].message.log[0].term);
}

TEST_F(ServerTests, LeaderAppendOnlyLogEntryAfterDiscoveringFollowerHasNoLogAtAllNopeNoSirIAmNewPleaseForgiveMe) {
    // Arrange
    AppendNoOpEntry(3);
    BecomeLeader(8);
    mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    messagesSent.clear();
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntriesResults;
    message.appendEntriesResults.term = 8;
    message.appendEntriesResults.success = false;
    message.appendEntriesResults.matchIndex = 0;
    server.ReceiveMessage(message.Serialize(), 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(Raft::Message::Type::AppendEntries, messagesSent[0].message.type);
    EXPECT_EQ(0, messagesSent[0].message.appendEntries.prevLogIndex);
    EXPECT_EQ(0, messagesSent[0].message.appendEntries.prevLogTerm);
    ASSERT_EQ(1, messagesSent[0].message.log.size());
    EXPECT_EQ(3, messagesSent[0].message.log[0].term);
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
    Raft::LogEntry secondEntry;
    secondEntry.term = 3;
    AppendNoOpEntry(2);
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
            EXPECT_EQ(2, messageSent.message.appendEntries.prevLogTerm);
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

    // Act
    CastVotes(1);

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

TEST_F(ServerTests, FollowerReceiveAppendEntriesSuccess) {
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
    ReceiveAppendEntriesFromMockLeader(2, 8, 1, 0, {newConflictingEntry, nextEntry}, false);

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
    Raft::LogEntry nextEntry;
    nextEntry.term = 8;
    mockLog->entries = {};
    MobilizeServer();
    ReceiveAppendEntriesFromMockLeader(2, 8);

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

    // Act
    RequestVote(2, 1, 0);

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

TEST_F(ServerTests, CrashedFollowerRestartsAndRejectsVoteFromDifferentCandidate) {
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

TEST_F(ServerTests, PersistentStateUpdateForNewTermWhenReceivingVoteRequestForNewCandidate) {
    // Arrange
    mockPersistentState->variables.currentTerm = 4;
    MobilizeServer();

    // Act
    RequestVote(2, 5, 0);

    // Assert
    EXPECT_EQ(5, mockPersistentState->variables.currentTerm);
}

TEST_F(ServerTests, PersistentStateUpdateForNewTermWhenVoteRejectedByNewerTermServer) {
    // Arrange
    BecomeCandidate(4);

    // Act
    CastVote(2, 5, false);

    // Assert
    EXPECT_EQ(5, mockPersistentState->variables.currentTerm);
}

TEST_F(ServerTests, PersistentStateUpdateForNewTermWhenReceiveAppendEntriesFromNewerTermLeader) {
    // Arrange
    mockPersistentState->variables.currentTerm = 4;
    MobilizeServer();

    // Act
    ReceiveAppendEntriesFromMockLeader(2, 5, 95);

    // Assert
    EXPECT_EQ(5, mockPersistentState->variables.currentTerm);
}

TEST_F(ServerTests, ApplyConfigVotingMemberSingleConfigOnStartup) {
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

TEST_F(ServerTests, DoNotCrashWhenCommittingSingleConfigFollowedByNoOp) {
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

TEST_F(ServerTests, ApplyConfigNonVotingMemberSingleConfigOnStartup) {
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
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_FALSE(server.IsVotingMember());
}

TEST_F(ServerTests, NonVotingMemberOnStartupNoLog) {
    // Arrange
    clusterConfiguration.instanceIds = {5, 6, 7, 11};
    serverConfiguration.selfInstanceId = 2;

    // Act
    MobilizeServer();
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_FALSE(server.IsVotingMember());
}

TEST_F(ServerTests, ApplyConfigNonVotingMemberSingleConfigWhenAppendedAsFollower) {
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

TEST_F(ServerTests, BecomeNonVotingMemberWhenLeaderAndSingleConfigAppendedAndNotInClusterAnymore) {
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

TEST_F(ServerTests, StepDownFromLeadershipOnceSingleConfigCommittedAndNotInClusterAnymore) {
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
    server.WaitForAtLeastOneWorkerLoop();

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
            Raft::Message message;
            message.type = Raft::Message::Type::AppendEntriesResults;
            message.appendEntriesResults.term = term;
            message.appendEntriesResults.success = true;
            message.appendEntriesResults.matchIndex = 1;
            ++responseCount;
            server.ReceiveMessage(message.Serialize(), instance);
            server.WaitForAtLeastOneWorkerLoop();
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

TEST_F(ServerTests, SendCorrectHeartBeatToNewServersOnceJointConfigApplied) {
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
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    bool newServerReceivedHeartBeat = false;
    for (const auto& messageSent: messagesSent) {
        if (
            (messageSent.message.type == Raft::Message::Type::AppendEntries)
            && (messageSent.receiverInstanceNumber == 12)
        ) {
            messagesSent.clear();
            newServerReceivedHeartBeat = true;
            Raft::Message message;
            message.type = Raft::Message::Type::AppendEntriesResults;
            message.appendEntriesResults.term = term;
            message.appendEntriesResults.success = false;
            message.appendEntriesResults.matchIndex = 0;
            server.ReceiveMessage(message.Serialize(), 12);
            server.WaitForAtLeastOneWorkerLoop();
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

TEST_F(ServerTests, VotingMemberFromBothConfigsJointConfig) {
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
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_TRUE(server.IsVotingMember());
}

TEST_F(ServerTests, VotingMemberFromNewConfigJointConfig) {
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
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_TRUE(server.IsVotingMember());
}

TEST_F(ServerTests, VotingMemberFromOldConfigJointConfig) {
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
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_TRUE(server.IsVotingMember());
}

TEST_F(ServerTests, NonVotingMemberJointConfig) {
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
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_FALSE(server.IsVotingMember());
}

TEST_F(ServerTests, NonVotingMemberShouldNotVoteForAnyCandidate) {
    // Arrange
    clusterConfiguration.instanceIds = {5, 6, 7, 11};
    serverConfiguration.selfInstanceId = 2;
    MobilizeServer();

    // Act
    RequestVote(2, 1, 0);

    // Assert
    ASSERT_TRUE(messagesSent.empty());
}

TEST_F(ServerTests, NonVotingMemberShouldNotStartNewElection) {
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

TEST_F(ServerTests, FollowerRevertConfigWhenRollingBackBeforeConfigChange) {
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

TEST_F(ServerTests, StartConfigurationProcessWhenLeader) {
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
    server.WaitForAtLeastOneWorkerLoop();

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

TEST_F(ServerTests, StartConfigurationProcessWhenNotLeader) {
    // Arrange
    serverConfiguration.selfInstanceId = 2;
    clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
    Raft::ClusterConfiguration newConfiguration(clusterConfiguration);
    newConfiguration.instanceIds = {2, 5, 6, 7, 12};
    MobilizeServer();
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    server.ChangeConfiguration(newConfiguration);
    mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_FALSE(server.HasJointConfiguration());
    EXPECT_EQ(
        Raft::IServer::ElectionState::Follower,
        server.GetElectionState()
    );
    EXPECT_TRUE(messagesSent.empty());
}

TEST_F(ServerTests, DoNotApplyJointConfigurationIfNewServersAreNotYetCaughtUp) {
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
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instanceNumber: clusterConfiguration.instanceIds) {
        if (instanceNumber == serverConfiguration.selfInstanceId) {
            continue;
        }
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntriesResults;
        message.appendEntriesResults.term = term;
        message.appendEntriesResults.success = true;
        message.appendEntriesResults.matchIndex = 1;
        server.ReceiveMessage(message.Serialize(), instanceNumber);
        server.WaitForAtLeastOneWorkerLoop();
    }

    // Act
    server.ChangeConfiguration(newConfiguration);
    messagesSent.clear();
    mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
    server.WaitForAtLeastOneWorkerLoop();

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

TEST_F(ServerTests, ApplyJointConfigurationOnceNewServersCaughtUp) {
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
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instanceNumber: clusterConfiguration.instanceIds) {
        if (instanceNumber == serverConfiguration.selfInstanceId) {
            continue;
        }
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntriesResults;
        message.appendEntriesResults.term = term;
        message.appendEntriesResults.success = true;
        message.appendEntriesResults.matchIndex = 1;
        server.ReceiveMessage(message.Serialize(), instanceNumber);
        server.WaitForAtLeastOneWorkerLoop();
    }

    // Act
    server.ChangeConfiguration(newConfiguration);
    mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instanceNumber: jointConfigurationNotIncludingSelfInstanceIds) {
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntriesResults;
        message.appendEntriesResults.term = term;
        message.appendEntriesResults.success = true;
        message.appendEntriesResults.matchIndex = 1;
        server.ReceiveMessage(message.Serialize(), instanceNumber);
        server.WaitForAtLeastOneWorkerLoop();
    }
    messagesSent.clear();
    mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
    server.WaitForAtLeastOneWorkerLoop();

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

TEST_F(ServerTests, ApplyNewConfigurationOnceJointConfigurationCommitted) {
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
    BecomeLeader(term);

    // Act
    for (auto instanceNumber: jointConfigurationNotIncludingSelfInstanceIds) {
        if (instanceNumber == serverConfiguration.selfInstanceId) {
            continue;
        }
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntriesResults;
        message.appendEntriesResults.term = term;
        message.appendEntriesResults.success = true;
        message.appendEntriesResults.matchIndex = 1;
        server.ReceiveMessage(message.Serialize(), instanceNumber);
        server.WaitForAtLeastOneWorkerLoop();
    }
    messagesSent.clear();
    mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
    server.WaitForAtLeastOneWorkerLoop();

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

TEST_F(ServerTests, DoNotApplySingleConfigurationWhenJointConfigurationCommittedIfSingleConfigurationAlreadyApplied) {
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
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntriesResults;
        message.appendEntriesResults.term = term;
        message.appendEntriesResults.success = true;
        message.appendEntriesResults.matchIndex = 1;
        server.ReceiveMessage(message.Serialize(), instanceNumber);
        server.WaitForAtLeastOneWorkerLoop();
    }
    messagesSent.clear();

    // Assert
    EXPECT_EQ(2, mockLog->entries.size());
}

TEST_F(ServerTests, JointConfigurationShouldBeCommittedIfMajorityAchievedByCommonServerRespondingLast) {
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
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntriesResults;
    message.appendEntriesResults.term = term;
    message.appendEntriesResults.success = true;
    message.appendEntriesResults.matchIndex = 1;
    server.ReceiveMessage(message.Serialize(), 7);
    server.ReceiveMessage(message.Serialize(), 6);

    // Act
    server.ReceiveMessage(message.Serialize(), 5);

    // Assert
    EXPECT_EQ(1, server.GetCommitIndex());
}

TEST_F(ServerTests, LeaderStepsDownIfNotInNewConfigurationOnceItIsCommitted) {
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
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntriesResults;
        message.appendEntriesResults.term = term;
        message.appendEntriesResults.success = true;
        message.appendEntriesResults.matchIndex = 1;
        server.ReceiveMessage(message.Serialize(), instanceNumber);
        server.WaitForAtLeastOneWorkerLoop();
    }
    messagesSent.clear();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Follower,
        server.GetElectionState()
    );
    EXPECT_FALSE(server.IsVotingMember());
}

TEST_F(ServerTests, LeaderMaintainsLeadershipIfInNewConfigurationOnceItIsCommitted) {
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
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntriesResults;
        message.appendEntriesResults.term = term;
        message.appendEntriesResults.success = true;
        message.appendEntriesResults.matchIndex = 1;
        server.ReceiveMessage(message.Serialize(), instanceNumber);
        server.WaitForAtLeastOneWorkerLoop();
    }
    messagesSent.clear();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Leader,
        server.GetElectionState()
    );
    EXPECT_TRUE(server.IsVotingMember());
}

TEST_F(ServerTests, JointConcensusIsNotAchievedSolelyFromSimpleMajority) {
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
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntriesResults;
        message.appendEntriesResults.term = term;
        if (idsOfInstancesSuccessfullyMatchingLog.find(instanceNumber) == idsOfInstancesSuccessfullyMatchingLog.end()) {
            message.appendEntriesResults.success = false;
            message.appendEntriesResults.matchIndex = 0;
        } else {
            message.appendEntriesResults.success = true;
            message.appendEntriesResults.matchIndex = 1;
        }
        server.ReceiveMessage(message.Serialize(), instanceNumber);
        server.WaitForAtLeastOneWorkerLoop();
    }

    // Assert
    EXPECT_EQ(0, server.GetCommitIndex());
}

TEST_F(ServerTests, CandidateNeedsSeparateMajoritiesToWinDuringJointConcensus) {
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

TEST_F(ServerTests, StaleAppendEntriesDeservesAFailureResponse) {
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
        messagesSent[0].message.appendEntriesResults.term
    );
    EXPECT_FALSE(messagesSent[0].message.appendEntriesResults.success);
}

TEST_F(ServerTests, ReceivingHeartBeatsDoesNotCausePersistentStateSaves) {
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

TEST_F(ServerTests, IgnoreAppendEntriesSameTermIfLeader) {
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

TEST_F(ServerTests, DoNotTellLogKeeperToCommitIfCommitIndexUnchanged) {
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

TEST_F(ServerTests, DoNotCommitToLogAnyEntriesWeDoNotHave) {
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

TEST_F(ServerTests, StaleServerShouldRevertToFollowerWhenAppendEntryResultsHigherTermReceived) {
    // Arrange
    constexpr int term = 7;
    BecomeLeader(term);
    mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2 + 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntriesResults;
    message.appendEntriesResults.term = term + 1;
    message.appendEntriesResults.success = false;
    message.appendEntriesResults.matchIndex = 0;
    server.ReceiveMessage(message.Serialize(), 2);

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Follower,
        server.GetElectionState()
    );
    EXPECT_EQ(term + 1, mockPersistentState->variables.currentTerm);
}

TEST_F(ServerTests, DoNotRetransmitRequestsToServersNoLongerInTheCluster) {
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

TEST_F(ServerTests, VotesShouldBeRequestedForNewServersWhenStartingElectionDuringJointConfiguration) {
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

TEST_F(ServerTests, NewLeaderShouldSendHeartBeatsImmediately) {
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

TEST_F(ServerTests, RemobilizeShouldClearJointConfigurationState) {
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
    server.WaitForAtLeastOneWorkerLoop();
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

TEST_F(ServerTests, RemobilizeShouldResetLastIndexCache) {
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

TEST_F(ServerTests, CallDelegateOnApplyConfiguration) {
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
    std::unique_ptr< Raft::ClusterConfiguration > configApplied;
    const auto onApplyConfiguration = [&configApplied](
        const Raft::ClusterConfiguration& newConfiguration
    ) {
        configApplied.reset(new Raft::ClusterConfiguration(newConfiguration));
    };
    server.SetApplyConfigurationDelegate(onApplyConfiguration);

    // Act
    MobilizeServer();

    // Assert
    ASSERT_FALSE(configApplied == nullptr);
    EXPECT_EQ(
        std::set< int >({2, 6, 7, 12}),
        configApplied->instanceIds
    );
}

TEST_F(ServerTests, CallDelegateOnCommitConfiguration) {
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
    std::unique_ptr< Raft::ClusterConfiguration > configCommitted;
    size_t commitLogIndex = 0;
    const auto onCommitConfiguration = [
        &configCommitted,
        &commitLogIndex
    ](
        const Raft::ClusterConfiguration& newConfiguration,
        size_t logIndex
    ) {
        configCommitted.reset(new Raft::ClusterConfiguration(newConfiguration));
        commitLogIndex = logIndex;
    };
    server.SetCommitConfigurationDelegate(onCommitConfiguration);
    BecomeLeader(term, false);
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntriesResults;
    message.appendEntriesResults.term = term;
    message.appendEntriesResults.success = true;
    message.appendEntriesResults.matchIndex = 2;

    // Act
    EXPECT_TRUE(configCommitted == nullptr);
    for (auto instance: clusterConfiguration.instanceIds) {
        if (instance != serverConfiguration.selfInstanceId) {
            server.ReceiveMessage(message.Serialize(), instance);
            server.WaitForAtLeastOneWorkerLoop();
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

TEST_F(ServerTests, DoNotCallCommitConfigurationDelegateWhenReplayingOldSingleConfigurationCommand) {
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
    bool configCommitted = false;
    const auto onCommitConfiguration = [&configCommitted](
        const Raft::ClusterConfiguration& newConfiguration,
        size_t logIndex
    ) {
        configCommitted = true;
    };
    server.SetCommitConfigurationDelegate(onCommitConfiguration);
    BecomeLeader(term, false);
    Raft::Message message;
    message.type = Raft::Message::Type::AppendEntriesResults;
    message.appendEntriesResults.term = term;
    message.appendEntriesResults.success = true;
    message.appendEntriesResults.matchIndex = 2;

    // Act
    for (auto instance: clusterConfiguration.instanceIds) {
        if (instance != serverConfiguration.selfInstanceId) {
            server.ReceiveMessage(message.Serialize(), instance);
            server.WaitForAtLeastOneWorkerLoop();
        }
    }

    // Assert
    EXPECT_FALSE(configCommitted);
}

TEST_F(ServerTests, VoteAfterElectionShouldNotPreventAppendEntriesRetransmission) {
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
