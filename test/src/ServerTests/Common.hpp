#pragma once

/**
 * @file Common.hpp
 *
 * This module declares the base fixture used to test the Raft::Server class.
 * The fixture is subclassed to test various aspects of the class, including:
 * - Elections: the process of selecting a cluster leader
 * - Replication: getting each server in the cluster to have the same log
 * - Reconfiguration: adding or removing servers in the cluster
 *
 * © 2019-2020 by Richard Walters
 */

#include "../../../src/Message.hpp"

#include <functional>
#include <future>
#include <gtest/gtest.h>
#include <Json/Value.hpp>
#include <mutex>
#include <Raft/ILog.hpp>
#include <Raft/IPersistentState.hpp>
#include <Raft/LogEntry.hpp>
#include <Raft/Server.hpp>
#include <stddef.h>
#include <Timekeeping/Scheduler.hpp>
#include <vector>

namespace ServerTests {

    /**
     * This is a fake time-keeper which is used to test the server.
     */
    struct MockTimeKeeper
        : public Timekeeping::Clock
    {
        // Properties

        double currentTime = 0.0;
        std::vector< std::function< void() > > destructionDelegates;

        // Lifecycle

        ~MockTimeKeeper();
        MockTimeKeeper(const MockTimeKeeper&) = delete;
        MockTimeKeeper(MockTimeKeeper&&) = delete;
        MockTimeKeeper& operator=(const MockTimeKeeper&) = delete;
        MockTimeKeeper& operator=(MockTimeKeeper&&) = delete;

        // Methods

        MockTimeKeeper() = default;

        void RegisterDestructionDelegate(std::function< void() > destructionDelegate);

        // Timekeeping::Clock

        virtual double GetCurrentTime() override;
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
        size_t baseIndex = 0;
        int baseTerm = 0;
        size_t commitIndex = 0;
        size_t commitCount = 0;
        Json::Value snapshot;
        std::vector< std::function< void() > > destructionDelegates;
        std::function< void() > onSnapshotInstalled;

        // Lifecycle

        ~MockLog();
        MockLog(const MockLog&) = delete;
        MockLog(MockLog&&) = delete;
        MockLog& operator=(const MockLog&) = delete;
        MockLog& operator=(MockLog&&) = delete;

        // Methods

        MockLog() = default;

        void RegisterDestructionDelegate(std::function< void() > destructionDelegate);

        // Raft::ILog

        virtual size_t GetSize() override;
        virtual size_t GetBaseIndex() override;
        virtual const Json::Value& GetSnapshot() override;
        virtual void InstallSnapshot(
            const Json::Value& snapshot,
            size_t lastIncludedIndex,
            int lastIncludedTerm
        ) override;
        virtual size_t GetLastIndex() override;
        virtual int GetTerm(size_t index) override;
        virtual const Raft::LogEntry& operator[](size_t index) override;
        virtual void RollBack(size_t index) override;
        virtual void Append(const std::vector< Raft::LogEntry >& newEntries) override;
        virtual void Commit(size_t index) override;
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

        ~MockPersistentState();
        MockPersistentState(const MockPersistentState&) = delete;
        MockPersistentState(MockPersistentState&&) = delete;
        MockPersistentState& operator=(const MockPersistentState&) = delete;
        MockPersistentState& operator=(MockPersistentState&&) = delete;

        // Methods

        MockPersistentState() = default;

        void RegisterDestructionDelegate(std::function< void() > destructionDelegate);

        // Raft::IPersistentState

        virtual Variables Load() override;
        virtual void Save(const Variables& newVariables) override;
    };

    /**
     * This holds information about a message received from the unit under
     * test.
     */
    struct MessageInfo {
        int receiverInstanceNumber;
        Raft::Message message;
    };

    /**
     * This is the base class for the concrete ServerTests test fixtures,
     * providing common setup and teardown for each test.
     */
    struct Common
        : public ::testing::Test
    {
        // Properties

        bool caughtUp = false;
        Raft::ClusterConfiguration clusterConfiguration;
        size_t commitLogIndex = 0;
        std::shared_ptr< std::promise< Raft::ClusterConfiguration > > configApplied;
        std::shared_ptr< std::promise< Raft::ClusterConfiguration > > configCommitted;
        std::vector< std::string > diagnosticMessages;
        SystemAbstractions::DiagnosticsSender::UnsubscribeDelegate diagnosticsUnsubscribeDelegate;
        std::vector< Json::Value > electionStateChanges;
        std::promise< void > electionStateChangesAwaited;
        Raft::IServer::EventsUnsubscribeDelegate eventsUnsubscribeDelegate;
        size_t lastIncludedIndexInSnapshot = 0;
        int lastIncludedTermInSnapshot = 0;
        std::shared_ptr< std::promise< void > > leadershipChangeAnnounced;
        struct {
            int leaderId = 0;
            int term = 0;
        } leadershipChangeDetails;
        std::promise< void > messagesAwaitedSent;
        std::vector< MessageInfo > messagesSent;
        std::shared_ptr< MockLog > mockLog = std::make_shared< MockLog >();
        std::shared_ptr< MockPersistentState > mockPersistentState = std::make_shared< MockPersistentState >();
        std::shared_ptr< MockTimeKeeper > mockTimeKeeper = std::make_shared< MockTimeKeeper >();
        std::mutex mutex;
        size_t numElectionStateChangesAwaiting = 0;
        size_t numMessagesAwaiting = 0;
        std::shared_ptr< Timekeeping::Scheduler > scheduler = std::make_shared< Timekeeping::Scheduler >();
        Raft::Server server;
        Raft::Server::ServerConfiguration serverConfiguration;
        Json::Value snapshotInstalled;

        // Methods

        template< typename T > bool Await(std::future< T >& future) {
            return (
                future.wait_for(
                    std::chrono::milliseconds(100)
                ) == std::future_status::ready
            );
        }

        std::future< void > SetUpToAwaitLeadershipChange();
        bool AwaitMessagesSent(size_t numMessages);
        bool AwaitElectionStateChanges(size_t numElectionStateChanges);
        void ServerSentMessage(
            const std::string& message,
            int receiverInstanceNumber
        );
        void MobilizeServer();
        void AdvanceTimeToJustBeforeElectionTimeout();
        void AppendNoOpEntry(int term);
        void CastVote(
            int instance,
            int term,
            bool granted = true
        );
        void CastVotes(
            int term,
            bool granted = true
        );
        void RequestVote(
            int instance,
            int term,
            int lastLogIndex
        );
        void RequestVote(
            int instance,
            int term,
            int lastLogIndex,
            int lastLogTerm
        );
        void ReceiveAppendEntriesFromMockLeader(
            int leaderId,
            int term,
            size_t leaderCommit,
            bool clearMessagesSent
        );
        void ReceiveAppendEntriesFromMockLeader(
            int leaderId,
            int term,
            bool clearMessagesSent = true
        );
        void ReceiveAppendEntriesFromMockLeader(
            int leaderId,
            int term,
            size_t leaderCommit,
            size_t prevLogIndex,
            const std::vector< Raft::LogEntry >& entries,
            bool clearMessagesSent = true
        );
        void ReceiveAppendEntriesFromMockLeader(
            int leaderId,
            int term,
            size_t leaderCommit,
            const std::vector< Raft::LogEntry >& entries,
            bool clearMessagesSent = true
        );
        void ReceiveAppendEntriesFromMockLeader(
            int leaderId,
            int term,
            const std::vector< Raft::LogEntry >& entries
        );
        void ReceiveAppendEntriesResults(
            int instance,
            int term,
            size_t matchIndex,
            bool success = true,
            int seq = 0
        );
        bool AwaitElectionTimeout(size_t messagesExpected = 4);
        void BecomeLeader(
            int term = 1,
            bool acknowledgeInitialHeartbeats = true
        );
        void BecomeCandidate(int term = 1);
        void SetServerDelegates();
        bool InstallSnapshotSent();

        // ::testing::Test

        virtual void SetUp() override;
        virtual void TearDown() override;
    };
}
