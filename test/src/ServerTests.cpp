/**
 * @file ServerTests.cpp
 *
 * This module contains the unit tests of the
 * Raft::Server class.
 *
 * © 2018 by Richard Walters
 */

#include <gtest/gtest.h>
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

    // Methods

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
    }

    virtual void TearDown() {
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
