#ifndef RAFT_TEST_SERVER_TESTS_COMMON_HPP
#define RAFT_TEST_SERVER_TESTS_COMMON_HPP

/**
 * @file Common.hpp
 *
 * This module declares the base fixture used to test the Raft::Server class.
 * The fixture is subclassed to test various aspects of the class, including:
 * - Elections: the process of selecting a cluster leader
 * - Replication: getting each server in the cluster to have the same log
 * - Reconfiguration: adding or removing servers in the cluster
 *
 * © 2019 by Richard Walters
 */

#include "../../../src/Message.hpp"

#include <functional>
#include <gtest/gtest.h>
#include <Json/Value.hpp>
#include <Raft/ILog.hpp>
#include <Raft/IPersistentState.hpp>
#include <Raft/LogEntry.hpp>
#include <Raft/Server.hpp>
#include <Raft/TimeKeeper.hpp>
#include <stddef.h>
#include <vector>

namespace ServerTests {

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

        ~MockTimeKeeper();
        MockTimeKeeper(const MockTimeKeeper&) = delete;
        MockTimeKeeper(MockTimeKeeper&&) = delete;
        MockTimeKeeper& operator=(const MockTimeKeeper&) = delete;
        MockTimeKeeper& operator=(MockTimeKeeper&&) = delete;

        // Methods

        MockTimeKeeper() = default;

        void RegisterDestructionDelegate(std::function< void() > destructionDelegate);

        // Http::TimeKeeper

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
        size_t commitIndex = 0;
        size_t commitCount = 0;
        std::vector< std::function< void() > > destructionDelegates;

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

        virtual size_t GetBaseIndex() override;
        virtual size_t GetLastIndex() override;
        virtual size_t GetSize() override;
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

        Raft::Server server;
        Raft::ClusterConfiguration clusterConfiguration;
        Raft::Server::ServerConfiguration serverConfiguration;
        std::vector< std::string > diagnosticMessages;
        SystemAbstractions::DiagnosticsSender::UnsubscribeDelegate diagnosticsUnsubscribeDelegate;
        std::shared_ptr< MockTimeKeeper > mockTimeKeeper = std::make_shared< MockTimeKeeper >();
        std::shared_ptr< MockLog > mockLog = std::make_shared< MockLog >();
        std::shared_ptr< MockPersistentState > mockPersistentState = std::make_shared< MockPersistentState >();
        std::vector< MessageInfo > messagesSent;
        bool leadershipChangeAnnounced = false;
        struct {
            int leaderId = 0;
            int term = 0;
        } leadershipChangeDetails;
        std::vector< Json::Value > electionStateChanges;
        bool caughtUp = false;

        // Methods

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
            size_t leaderCommit
        );
        void ReceiveAppendEntriesFromMockLeader(
            int leaderId,
            int term
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
            bool success = true
        );
        void WaitForElectionTimeout();
        void BecomeLeader(
            int term = 1,
            bool acknowledgeInitialHeartbeats = true
        );
        void BecomeCandidate(int term = 1);
        void SetServerDelegates();

        // ::testing::Test

        virtual void SetUp() override;
        virtual void TearDown() override;
    };
}

#endif /* RAFT_TEST_SERVER_TESTS_COMMON_HPP */
