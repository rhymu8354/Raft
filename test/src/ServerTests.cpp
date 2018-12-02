/**
 * @file ServerTests.cpp
 *
 * This module contains the unit tests of the
 * Raft::Server class.
 *
 * © 2018 by Richard Walters
 */

#include <src/MessageImpl.hpp>

#include <future>
#include <gtest/gtest.h>
#include <Raft/Message.hpp>
#include <Raft/Server.hpp>
#include <Raft/TimeKeeper.hpp>
#include <SystemAbstractions/StringExtensions.hpp>

namespace {

    /**
     * This is a fake time-keeper which is used to test the server.
     */
    struct MockTimeKeeper
        : public Raft::TimeKeeper
    {
        // Properties

        double currentTime = 0.0;

        // Methods

        // Http::TimeKeeper

        virtual double GetCurrentTime() override {
            return currentTime;
        }
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
    std::vector< std::string > diagnosticMessages;
    SystemAbstractions::DiagnosticsSender::UnsubscribeDelegate diagnosticsUnsubscribeDelegate;
    const std::shared_ptr< MockTimeKeeper > mockTimeKeeper = std::make_shared< MockTimeKeeper >();
    std::promise< std::shared_ptr< Raft::Message > > beginElection;
    bool beginElectionWasSet = false;

    // Methods

    void ServerSentMessage(std::shared_ptr< Raft::Message > message) {
        if (message->impl_->type == Raft::MessageImpl::Type::Election) {
            if (!beginElectionWasSet) {
                beginElectionWasSet = true;
                beginElection.set_value(message);
            }
        }
    }

    // ::testing::Test

    virtual void SetUp() {
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
                std::shared_ptr< Raft::Message > message
            ){
                ServerSentMessage(message);
            }
        );
    }

    virtual void TearDown() {
        server.Demobilize();
        diagnosticsUnsubscribeDelegate();
    }
};

TEST_F(ServerTests, InitialConfiguration) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;

    // Act
    server.Configure(configuration);

    // Assert
    const auto actualConfiguration = server.GetConfiguration();
    EXPECT_EQ(
        configuration.instanceNumbers,
        actualConfiguration.instanceNumbers
    );
    EXPECT_EQ(
        actualConfiguration.selfInstanceNumber,
        configuration.selfInstanceNumber
    );
}

TEST_F(ServerTests, ElectionStartedAfterProperTimeoutInterval) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumTimeout = 0.1;
    configuration.maximumTimeout = 0.2;
    server.Configure(configuration);

    // Act
    server.Mobilize();
    auto electionBegan = beginElection.get_future();
    mockTimeKeeper->currentTime = 0.0999;
    server.WaitForAtLeastOneWorkerLoop();
    EXPECT_NE(
        std::future_status::ready,
        electionBegan.wait_for(std::chrono::milliseconds(0))
    );
    mockTimeKeeper->currentTime = 0.2;
    EXPECT_EQ(
        std::future_status::ready,
        electionBegan.wait_for(std::chrono::milliseconds(1000))
    );

    // Assert
}

TEST_F(ServerTests, ServerVotesForItselfInElectionItStarts) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumTimeout = 0.1;
    configuration.maximumTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    auto electionBegan = beginElection.get_future();
    mockTimeKeeper->currentTime = 0.2;
    const auto message = electionBegan.get();

    // Assert
    EXPECT_EQ(5, message->impl_->election.candidateId);
}

TEST_F(ServerTests, ServerIncrementsTermInElectionItStarts) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumTimeout = 0.1;
    configuration.maximumTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    auto electionBegan = beginElection.get_future();
    mockTimeKeeper->currentTime = 0.2;
    const auto message = electionBegan.get();

    // Assert
    EXPECT_EQ(1, message->impl_->election.term);
}
