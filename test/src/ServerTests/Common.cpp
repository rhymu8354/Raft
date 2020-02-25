/**
 * @file Common.cpp
 *
 * This module provides the implementation of the base fixture used to test the
 * Raft::Server class.
 *
 * © 2019-2020 by Richard Walters
 */

#include "Common.hpp"

#include <algorithm>
#include <functional>
#include <gtest/gtest.h>
#include <Raft/ILog.hpp>
#include <Raft/IPersistentState.hpp>
#include <Raft/LogEntry.hpp>
#include <Raft/Server.hpp>
#include <stddef.h>
#include <StringExtensions/StringExtensions.hpp>
#include <vector>

namespace ServerTests {

    MockTimeKeeper::~MockTimeKeeper() {
        for (const auto& destructionDelegate: destructionDelegates) {
            destructionDelegate();
        }
    }
    void MockTimeKeeper::RegisterDestructionDelegate(std::function< void() > destructionDelegate) {
        destructionDelegates.push_back(destructionDelegate);
    }
    double MockTimeKeeper::GetCurrentTime() {
        return currentTime;
    }

    MockLog::~MockLog() {
        for (const auto& destructionDelegate: destructionDelegates) {
            destructionDelegate();
        }
    }

    void MockLog::RegisterDestructionDelegate(std::function< void() > destructionDelegate) {
        destructionDelegates.push_back(destructionDelegate);
    }

    size_t MockLog::GetSize() {
        return entries.size();
    }

    size_t MockLog::GetBaseIndex() {
        return baseIndex;
    }

    const Json::Value& MockLog::GetSnapshot() {
        return snapshot;
    }

    void MockLog::InstallSnapshot(
        const Json::Value& snapshot,
        size_t lastIncludedIndex,
        int lastIncludedTerm
    ) {
        this->snapshot = snapshot;
        if (baseIndex < lastIncludedIndex) {
            entries.clear();
        }
        baseIndex = lastIncludedIndex;
        commitIndex = lastIncludedIndex;
        baseTerm = lastIncludedTerm;
        if (onSnapshotInstalled != nullptr) {
            onSnapshotInstalled();
        }
    }

    size_t MockLog::GetLastIndex() {
        return baseIndex + entries.size();
    }

    int MockLog::GetTerm(size_t index) {
        if (index > baseIndex + entries.size()) {
            return 0;
        }
        return (
            (index <= baseIndex)
            ? baseTerm
            : entries[index - baseIndex - 1].term
        );
    }

    const Raft::LogEntry& MockLog::operator[](size_t index) {
        if (
            (index <= baseIndex)
            || (index > baseIndex + entries.size())
        ) {
            invalidEntryIndexed = true;
            static Raft::LogEntry outOfRangeReturnValue;
            return outOfRangeReturnValue;
        }
        return entries[index - baseIndex - 1];
    }

    void MockLog::RollBack(size_t index) {
        entries.resize(index);
    }

    void MockLog::Append(const std::vector< Raft::LogEntry >& newEntries) {
        std::copy(
            newEntries.begin(),
            newEntries.end(),
            std::back_inserter(entries)
        );
    }

    void MockLog::Commit(size_t index) {
        commitIndex = index;
        ++commitCount;
    }

    MockPersistentState::~MockPersistentState() {
        for (const auto& destructionDelegate: destructionDelegates) {
            destructionDelegate();
        }
    }

    void MockPersistentState::RegisterDestructionDelegate(std::function< void() > destructionDelegate) {
        destructionDelegates.push_back(destructionDelegate);
    }

    auto MockPersistentState::Load() -> Variables {
        return variables;
    }

    void MockPersistentState::Save(const Variables& newVariables) {
        variables = newVariables;
        ++saveCount;
    }

    std::future< void > Common::SetUpToAwaitLeadershipChange() {
        leadershipChangeAnnounced = std::make_shared< std::promise< void > >();
        return leadershipChangeAnnounced->get_future();
    }

    bool Common::Await(std::future< void >& future) {
        return (
            future.wait_for(
                std::chrono::milliseconds(100)
            ) == std::future_status::ready
        );
    }

    bool Common::AwaitEventsSent(size_t numEventsToAwait) {
        std::unique_lock< decltype(mutex) > lock(mutex);
        if (numEvents >= numEventsToAwait) {
            return true;
        }
        eventsAwaitedSent = std::promise< void >();
        numEventsAwaiting = numEventsToAwait - numEvents;
        lock.unlock();
        const auto result = Await(eventsAwaitedSent.get_future());
        lock.lock();
        return result;
    }

    bool Common::AwaitMessagesSent(size_t numMessages) {
        std::unique_lock< decltype(mutex) > lock(mutex);
        if (messagesSent.size() >= numMessages) {
            return true;
        }
        messagesAwaitedSent = std::promise< void >();
        numMessagesAwaiting = numMessages;
        lock.unlock();
        const auto result = Await(messagesAwaitedSent.get_future());
        lock.lock();
        numMessagesAwaiting = 0;
        return result;
    }

    bool Common::AwaitElectionStateChanges(size_t numElectionStateChanges) {
        std::unique_lock< decltype(mutex) > lock(mutex);
        if (electionStateChanges.size() >= numElectionStateChanges) {
            return true;
        }
        electionStateChangesAwaited = std::promise< void >();
        numElectionStateChangesAwaiting = numElectionStateChanges;
        lock.unlock();
        const auto result = Await(electionStateChangesAwaited.get_future());
        lock.lock();
        numElectionStateChanges = 0;
        return result;
    }

    void Common::ServerSentMessage(
        const std::string& message,
        int receiverInstanceNumber
    ) {
        MessageInfo messageInfo;
        messageInfo.message = message;
        messageInfo.receiverInstanceNumber = receiverInstanceNumber;
        messagesSent.push_back(std::move(messageInfo));
        if (
            (numMessagesAwaiting > 0)
            && (messagesSent.size() == numMessagesAwaiting)
        ) {
            messagesAwaitedSent.set_value();
        }
    }

    void Common::MobilizeServer() {
        server.Mobilize(
            mockLog,
            mockPersistentState,
            scheduler,
            clusterConfiguration,
            serverConfiguration
        );
    }

    void Common::AdvanceTimeToJustBeforeElectionTimeout() {
        mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout - 0.001;
        scheduler->WakeUp();
    }

    void Common::AppendNoOpEntry(int term) {
        Raft::LogEntry entry;
        entry.term = term;
        mockLog->entries.push_back(std::move(entry));
    }

    void Common::CastVote(
        int instance,
        int term,
        bool granted
    ) {
        if (instance != serverConfiguration.selfInstanceId) {
            Raft::Message message;
            message.type = Raft::Message::Type::RequestVoteResults;
            message.term = term;
            message.requestVoteResults.voteGranted = granted;
            server.ReceiveMessage(message.Serialize(), instance);
        }
    }

    void Common::CastVotes(
        int term,
        bool granted
    ) {
        for (auto instance: clusterConfiguration.instanceIds) {
            CastVote(instance, term, granted);
        }
    }

    void Common::RequestVote(
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

    void Common::RequestVote(
        int instance,
        int term,
        int lastLogIndex,
        int lastLogTerm
    ) {
        Raft::Message message;
        message.type = Raft::Message::Type::RequestVote;
        message.term = term;
        message.requestVote.candidateId = instance;
        message.requestVote.lastLogTerm = lastLogTerm;
        message.requestVote.lastLogIndex = lastLogIndex;
        server.ReceiveMessage(message.Serialize(), instance);
    }

    void Common::ReceiveAppendEntriesFromMockLeader(
        int leaderId,
        int term,
        size_t leaderCommit,
        bool clearMessagesSent
    ) {
        messagesSent.clear();
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntries;
        message.term = term;
        message.appendEntries.leaderCommit = leaderCommit;
        message.appendEntries.prevLogIndex = mockLog->baseIndex + mockLog->entries.size();
        message.appendEntries.prevLogTerm = (
            mockLog->entries.empty()
            ? 0
            : term
        );
        server.ReceiveMessage(message.Serialize(), leaderId);
        if (clearMessagesSent) {
            (void)AwaitMessagesSent(1);
            messagesSent.clear();
        }
    }

    void Common::ReceiveAppendEntriesFromMockLeader(
        int leaderId,
        int term,
        bool clearMessagesSent
    ) {
        ReceiveAppendEntriesFromMockLeader(
            leaderId,
            term,
            mockLog->baseIndex + mockLog->entries.size(),
            clearMessagesSent
        );
    }

    void Common::ReceiveAppendEntriesFromMockLeader(
        int leaderId,
        int term,
        size_t leaderCommit,
        size_t prevLogIndex,
        const std::vector< Raft::LogEntry >& entries,
        bool clearMessagesSent
    ) {
        messagesSent.clear();
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntries;
        message.term = term;
        message.appendEntries.leaderCommit = leaderCommit;
        message.appendEntries.prevLogIndex = prevLogIndex;
        if (prevLogIndex <= mockLog->baseIndex) {
            message.appendEntries.prevLogTerm = mockLog->baseTerm;
        } else {
            message.appendEntries.prevLogTerm = mockLog->entries[prevLogIndex - mockLog->baseIndex - 1].term;
        }
        message.log = entries;
        server.ReceiveMessage(message.Serialize(), leaderId);
        if (clearMessagesSent) {
            (void)AwaitMessagesSent(1);
            messagesSent.clear();
        }
    }

    void Common::ReceiveAppendEntriesFromMockLeader(
        int leaderId,
        int term,
        size_t leaderCommit,
        const std::vector< Raft::LogEntry >& entries,
        bool clearMessagesSent
    ) {
        ReceiveAppendEntriesFromMockLeader(
            leaderId,
            term,
            leaderCommit,
            mockLog->baseIndex + mockLog->entries.size(),
            entries,
            clearMessagesSent
        );
    }

    void Common::ReceiveAppendEntriesFromMockLeader(
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

    void Common::ReceiveAppendEntriesResults(
        int instance,
        int term,
        size_t matchIndex,
        bool success,
        int seq
    ) {
        Raft::Message message;
        message.type = Raft::Message::Type::AppendEntriesResults;
        message.term = term;
        message.seq = seq;
        message.appendEntriesResults.success = success;
        message.appendEntriesResults.matchIndex = matchIndex;
        server.ReceiveMessage(message.Serialize(), instance);
    }

    bool Common::AwaitElectionTimeout() {
        messagesSent.clear();
        mockTimeKeeper->currentTime += serverConfiguration.maximumElectionTimeout;
        scheduler->WakeUp();
        return AwaitMessagesSent(4);
    }

    void Common::BecomeLeader(
        int term,
        bool acknowledgeInitialHeartbeats
    ) {
        mockPersistentState->variables.currentTerm = term - 1;
        MobilizeServer();
        (void)AwaitElectionTimeout();
        CastVotes(term);
        (void)AwaitMessagesSent(4);
        messagesSent.clear();
        if (acknowledgeInitialHeartbeats) {
            for (auto instance: clusterConfiguration.instanceIds) {
                if (instance != serverConfiguration.selfInstanceId) {
                    ReceiveAppendEntriesResults(instance, term, mockLog->baseIndex + mockLog->entries.size());
                }
            }
        }
    }

    void Common::BecomeCandidate(int term) {
        mockPersistentState->variables.currentTerm = term - 1;
        MobilizeServer();
        (void)AwaitElectionTimeout();
        messagesSent.clear();
    }

    void Common::SetServerDelegates() {
        scheduler->SetClock(mockTimeKeeper);
        diagnosticsUnsubscribeDelegate = server.SubscribeToDiagnostics(
            [this](
                std::string senderName,
                size_t level,
                std::string message
            ){
                diagnosticMessages.push_back(
                    StringExtensions::sprintf(
                        "%s[%zu]: %s",
                        senderName.c_str(),
                        level,
                        message.c_str()
                    )
                );
            },
            0
        );
        eventsUnsubscribeDelegate = server.SubscribeToEvents(
            [this](
                const Raft::IServer::Event& baseEvent
            ){
                std::lock_guard< decltype(mutex) > lock(mutex);
                switch (baseEvent.type) {
                    case Raft::IServer::Event::Type::SendMessage: {
                        const auto& event = static_cast< const Raft::IServer::SendMessageEvent& >(baseEvent);
                        ServerSentMessage(event.serializedMessage, event.receiverInstanceNumber);
                    } break;

                    case Raft::IServer::Event::Type::LeadershipChange: {
                        const auto& event = static_cast< const Raft::IServer::LeadershipChangeEvent& >(baseEvent);
                        if (leadershipChangeAnnounced != nullptr) {
                            leadershipChangeAnnounced->set_value();
                            leadershipChangeAnnounced = nullptr;
                        }
                        leadershipChangeDetails.leaderId = event.leaderId;
                        leadershipChangeDetails.term = event.term;
                    } break;

                    case Raft::IServer::Event::Type::ElectionState: {
                        const auto& event = static_cast< const Raft::IServer::ElectionStateEvent& >(baseEvent);
                        std::string electionStateAsString;
                        switch (event.electionState) {
                            case Raft::IServer::ElectionState::Follower: {
                                electionStateAsString = "follower";
                            } break;
                            case Raft::IServer::ElectionState::Candidate: {
                                electionStateAsString = "candidate";
                            } break;
                            case Raft::IServer::ElectionState::Leader: {
                                electionStateAsString = "leader";
                            } break;
                            default: {
                                electionStateAsString = "???";
                            } break;
                        }
                        electionStateChanges.push_back(
                            Json::Object({
                                {"term", event.term},
                                {"electionState", electionStateAsString},
                                {"didVote", event.didVote},
                                {"votedFor", event.votedFor},
                            })
                        );
                        if (
                            (numElectionStateChangesAwaiting > 0)
                            && (electionStateChanges.size() == numElectionStateChangesAwaiting)
                        ) {
                            electionStateChangesAwaited.set_value();
                        }
                    } break;

                    case Raft::IServer::Event::Type::ApplyConfiguration: {
                        const auto& event = static_cast< const Raft::IServer::ApplyConfigurationEvent& >(baseEvent);
                        configApplied.reset(new Raft::ClusterConfiguration(event.newConfig));
                    } break;

                    case Raft::IServer::Event::Type::CommitConfiguration: {
                        const auto& event = static_cast< const Raft::IServer::CommitConfigurationEvent& >(baseEvent);
                        configCommitted.reset(new Raft::ClusterConfiguration(event.newConfig));
                        commitLogIndex = event.logIndex;
                    } break;

                    case Raft::IServer::Event::Type::SnapshotInstalled: {
                        const auto& event = static_cast< const Raft::IServer::SnapshotInstalledEvent& >(baseEvent);
                        snapshotInstalled = event.snapshot;
                        lastIncludedIndexInSnapshot = event.lastIncludedIndex;
                        lastIncludedTermInSnapshot = event.lastIncludedTerm;
                    } break;

                    case Raft::IServer::Event::Type::CaughtUp: {
                        caughtUp = true;
                    } break;

                    default: break;
                }
                ++numEvents;
                if (numEventsAwaiting > 0) {
                    if (--numEventsAwaiting == 0) {
                        eventsAwaitedSent.set_value();
                    }
                }
            }
        );
    }

    bool Common::InstallSnapshotSent() {
        for (const auto& messageSent: messagesSent) {
            if (messageSent.message.type == Raft::Message::Type::InstallSnapshot) {
                return true;
            }
        }
        return false;
    }

    void Common::SetUp() {
        SetServerDelegates();
        clusterConfiguration.instanceIds = {2, 5, 6, 7, 11};
        serverConfiguration.selfInstanceId = 5;
        serverConfiguration.minimumElectionTimeout = 0.1;
        serverConfiguration.maximumElectionTimeout = 0.2;
        serverConfiguration.heartbeatInterval = 0.05;
        serverConfiguration.rpcTimeout = 0.01;
        mockPersistentState->variables.currentTerm = 0;
        mockPersistentState->variables.votedThisTerm = false;
    }

    void Common::TearDown() {
        server.Demobilize();
        eventsUnsubscribeDelegate();
        diagnosticsUnsubscribeDelegate();
    }

    /**
     * This is the test fixture for these tests, providing common
     * setup and teardown for each test.
     */
    struct ServerTests
        : public Common
    {
    };

    TEST_F(ServerTests, Mobilize_Twice_Does_Not_Crash) {
        // Arrange
        MobilizeServer();

        // Act
        MobilizeServer();
    }

    TEST_F(ServerTests, Log_Keeper_Released_On_Demobilize) {
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

    TEST_F(ServerTests, Persistent_State_Released_On_Demobilize) {
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

    TEST_F(ServerTests, Scheduler_Released_On_Demobilize) {
        // Arrange
        bool timeKeeperDestroyed = false;
        const auto onTimeKeeperDestroyed = [&timeKeeperDestroyed]{
            timeKeeperDestroyed = true;
        };
        mockTimeKeeper->RegisterDestructionDelegate(onTimeKeeperDestroyed);
        MobilizeServer();
        scheduler = nullptr;
        mockTimeKeeper = nullptr;

        // Act
        server.Demobilize();
        server = Raft::Server();

        // Assert
        EXPECT_TRUE(timeKeeperDestroyed);
    }

    TEST_F(ServerTests, Server_Does_Not_Retransmit_Too_Quickly) {
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
        mockTimeKeeper->currentTime += serverConfiguration.rpcTimeout - 0.0001;
        scheduler->WakeUp();

        // Assert
        EXPECT_FALSE(AwaitMessagesSent(1));
    }

    TEST_F(ServerTests, Server_Regular_Retransmissions) {
        // Arrange
        //
        // In this scenario, server 5 is a candidate, and receives
        // one less than the minimum number of votes required to
        // be leader.  One server (2) has not yet cast their vote.
        //
        // Server 5 should retransmit a vote request to server 2 every time the
        // retransmission time (rpcTimeout) has elapsed.
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
        mockTimeKeeper->currentTime = (
            serverConfiguration.maximumElectionTimeout
            + serverConfiguration.rpcTimeout
        );
        scheduler->WakeUp();
        EXPECT_TRUE(AwaitMessagesSent(1));
        mockTimeKeeper->currentTime = (serverConfiguration.maximumElectionTimeout
            + serverConfiguration.rpcTimeout * 2 - 0.001
        );
        scheduler->WakeUp();
        EXPECT_FALSE(AwaitMessagesSent(2));
        mockTimeKeeper->currentTime = (serverConfiguration.maximumElectionTimeout
            + serverConfiguration.rpcTimeout * 2 + 0.001
        );
        scheduler->WakeUp();
        EXPECT_TRUE(AwaitMessagesSent(2));
        mockTimeKeeper->currentTime = (serverConfiguration.maximumElectionTimeout
            + serverConfiguration.rpcTimeout * 3 - 0.001
        );
        scheduler->WakeUp();
        EXPECT_FALSE(AwaitMessagesSent(3));
        mockTimeKeeper->currentTime = (serverConfiguration.maximumElectionTimeout
            + serverConfiguration.rpcTimeout * 3 + 0.001
        );
        scheduler->WakeUp();
        EXPECT_TRUE(AwaitMessagesSent(3));

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
                messageSent.message.term
            );
        }
    }

    TEST_F(ServerTests, Update_Term_When_Receiving_Heart_Beat) {
        // Arrange
        MobilizeServer();

        // Act
        ReceiveAppendEntriesFromMockLeader(2, 2);

        // Assert
        EXPECT_EQ(2, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests, Receiving_First_Heart_Beat_As_Follower_Same_Term) {
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

    TEST_F(ServerTests, Receiving_First_Heart_Beat_As_Follower_Newer_Term) {
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

    TEST_F(ServerTests, Receiving_Two_Heart_Beats_As_Follower_Sequential_Terms) {
        // Arrange
        constexpr int firstLeaderId = 2;
        constexpr int secondLeaderId = 11;
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

    TEST_F(ServerTests, Receiving_Heart_Beat_From_Same_Term_Should_Reset_Election_Timeout) {
        // Arrange
        MobilizeServer();

        // Act
        while (mockTimeKeeper->currentTime <= serverConfiguration.maximumElectionTimeout * 2) {
            ReceiveAppendEntriesFromMockLeader(2, 2);
            mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2;
            scheduler->WakeUp();
            EXPECT_EQ(
                Raft::Server::ElectionState::Follower,
                server.GetElectionState()
            );
        }

        // Assert
        EXPECT_FALSE(AwaitMessagesSent(1)); // expect NO election
        EXPECT_EQ(
            Raft::Server::ElectionState::Follower,
            server.GetElectionState()
        );
    }

    TEST_F(ServerTests, Reply_Not_Success_For_Heart_Beat_From_Old_Term) {
        // Arrange
        MobilizeServer();
        ReceiveAppendEntriesFromMockLeader(2, 2);

        // Act
        ReceiveAppendEntriesFromMockLeader(2, 1, false);

        // Assert
        ASSERT_TRUE(AwaitMessagesSent(1));
        ASSERT_EQ(
            Raft::Message::Type::AppendEntriesResults,
            messagesSent[0].message.type
        );
        EXPECT_FALSE(messagesSent[0].message.appendEntriesResults.success);
        EXPECT_EQ(2, mockPersistentState->variables.currentTerm);
    }

    TEST_F(ServerTests, ReceivingHeartBeatFromOldTermShouldNotResetElectionTimeout) {
        // Arrange
        mockPersistentState->variables.currentTerm = 42;
        MobilizeServer();

        // Act
        while (mockTimeKeeper->currentTime <= serverConfiguration.maximumElectionTimeout * 2) {
            Raft::Message message;
            ReceiveAppendEntriesFromMockLeader(2, 13);
            mockTimeKeeper->currentTime += serverConfiguration.minimumElectionTimeout / 2;
            scheduler->WakeUp();
        }

        // Assert
        EXPECT_TRUE(AwaitMessagesSent(1));
    }

}
