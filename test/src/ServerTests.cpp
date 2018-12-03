/**
 * @file ServerTests.cpp
 *
 * This module contains the unit tests of the
 * Raft::Server class.
 *
 * © 2018 by Richard Walters
 */

#include <src/MessageImpl.hpp>

#include <condition_variable>
#include <gtest/gtest.h>
#include <mutex>
#include <Raft/Message.hpp>
#include <Raft/Server.hpp>
#include <Raft/TimeKeeper.hpp>
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

        // Methods

        // Http::TimeKeeper

        virtual double GetCurrentTime() override {
            return currentTime;
        }
    };

    /**
     * This holds information about a message received from the unit under
     * test.
     */
    struct MessageInfo {
        unsigned int receiverInstanceNumber;
        std::shared_ptr< Raft::Message > message;
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
    std::vector< MessageInfo > messagesSent;
    std::mutex messagesSentMutex;
    std::condition_variable messagesSentCondition;

    // Methods

    bool AwaitServerMessagesToBeSent(size_t numMessages) {
        std::unique_lock< decltype(messagesSentMutex) > lock(messagesSentMutex);
        return messagesSentCondition.wait_for(
            lock,
            std::chrono::milliseconds(1000),
            [this, numMessages]{ return messagesSent.size() >= numMessages; }
        );
    }

    void ServerSentMessage(
        std::shared_ptr< Raft::Message > message,
        unsigned int receiverInstanceNumber
    ) {
        std::lock_guard< decltype(messagesSentMutex) > lock(messagesSentMutex);
        MessageInfo messageInfo;
        messageInfo.message = message;
        messageInfo.receiverInstanceNumber = receiverInstanceNumber;
        messagesSent.push_back(std::move(messageInfo));
        messagesSentCondition.notify_one();
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
                std::shared_ptr< Raft::Message > message,
                unsigned int receiverInstanceNumber
            ){
                ServerSentMessage(message, receiverInstanceNumber);
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
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);

    // Act
    server.Mobilize();
    mockTimeKeeper->currentTime = 0.0999;
    server.WaitForAtLeastOneWorkerLoop();
    EXPECT_EQ(0, messagesSent.size());
    mockTimeKeeper->currentTime = 0.2;
    EXPECT_TRUE(AwaitServerMessagesToBeSent(4));

    // Assert
    for (const auto messageInfo: messagesSent) {
        EXPECT_EQ(
            Raft::MessageImpl::Type::RequestVote,
            messageInfo.message->impl_->type
        );
    }
}

TEST_F(ServerTests, RequestVoteNotSentToAllServersExceptSelf) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    mockTimeKeeper->currentTime = 0.2;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(4, messagesSent.size());
    std::set< unsigned int > instances(
        configuration.instanceNumbers.begin(),
        configuration.instanceNumbers.end()
    );
    for (const auto messageInfo: messagesSent) {
        (void)instances.erase(messageInfo.receiverInstanceNumber);
    }
    EXPECT_EQ(
        std::set< unsigned int >{ configuration.selfInstanceNumber },
        instances
    );
}

TEST_F(ServerTests, ServerVotesForItselfInElectionItStarts) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    mockTimeKeeper->currentTime = 0.2;
    (void)AwaitServerMessagesToBeSent(4);

    // Assert
    for (auto messageInfo: messagesSent) {
        EXPECT_EQ(5, messageInfo.message->impl_->requestVote.candidateId);
    }
}

TEST_F(ServerTests, ServerIncrementsTermInElectionItStarts) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    mockTimeKeeper->currentTime = 0.2;
    (void)AwaitServerMessagesToBeSent(4);

    // Assert
    for (const auto messageInfo: messagesSent) {
        EXPECT_EQ(1, messageInfo.message->impl_->requestVote.term);
    }
}

TEST_F(ServerTests, ServerDoesReceiveUnanimousVoteInElection) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = 0.2;
    (void)AwaitServerMessagesToBeSent(4);

    // Act
    for (auto instance: configuration.instanceNumbers) {
        if (instance != configuration.selfInstanceNumber) {
            const auto message = Raft::Message::CreateMessage();
            message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
            message->impl_->requestVoteResults.term = 0;
            message->impl_->requestVoteResults.voteGranted = true;
            server.ReceiveMessage(message, instance);
        }
    }

    // Assert
    EXPECT_TRUE(server.IsLeader());
}

TEST_F(ServerTests, ServerDoesReceiveNonUnanimousMajorityVoteInElection) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = 0.2;
    (void)AwaitServerMessagesToBeSent(4);

    // Act
    for (auto instance: configuration.instanceNumbers) {
        if (instance != configuration.selfInstanceNumber) {
            const auto message = Raft::Message::CreateMessage();
            message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
            if (instance == 11) {
                message->impl_->requestVoteResults.term = 1;
                message->impl_->requestVoteResults.voteGranted = false;
            } else {
                message->impl_->requestVoteResults.term = 0;
                message->impl_->requestVoteResults.voteGranted = true;
            }
            server.ReceiveMessage(message, instance);
        }
    }

    // Assert
    EXPECT_TRUE(server.IsLeader());
}

TEST_F(ServerTests, ServerRetransmitsRequestVoteForSlowVotersInElection) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    configuration.rpcTimeout = 0.01;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    (void)AwaitServerMessagesToBeSent(4);
    for (auto instance: configuration.instanceNumbers) {
        switch (instance) {
            case 6:
            case 7:
            case 11: {
                const auto message = Raft::Message::CreateMessage();
                message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
                if (instance == 11) {
                    message->impl_->requestVoteResults.term = 1;
                    message->impl_->requestVoteResults.voteGranted = false;
                } else {
                    message->impl_->requestVoteResults.term = 0;
                    message->impl_->requestVoteResults.voteGranted = true;
                }
                server.ReceiveMessage(message, instance);
            }
        }
    }

    // Act
    messagesSent.clear();
    mockTimeKeeper->currentTime += configuration.rpcTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_GE(messagesSent.size(), 1);
    EXPECT_EQ(1, messagesSent.size());
    EXPECT_EQ(2, messagesSent[0].receiverInstanceNumber);
    EXPECT_EQ(
        Raft::MessageImpl::Type::RequestVote,
        messagesSent[0].message->impl_->type
    );
    EXPECT_EQ(
        configuration.selfInstanceNumber,
        messagesSent[0].message->impl_->requestVote.candidateId
    );
    EXPECT_EQ(
        1,
        messagesSent[0].message->impl_->requestVote.term
    );
}

TEST_F(ServerTests, ServerDoesNotRetransmitTooQuickly) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    configuration.rpcTimeout = 0.01;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    (void)AwaitServerMessagesToBeSent(4);
    for (auto instance: configuration.instanceNumbers) {
        switch (instance) {
            case 6:
            case 7:
            case 11: {
                const auto message = Raft::Message::CreateMessage();
                message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
                if (instance == 11) {
                    message->impl_->requestVoteResults.term = 1;
                    message->impl_->requestVoteResults.voteGranted = false;
                } else {
                    message->impl_->requestVoteResults.term = 0;
                    message->impl_->requestVoteResults.voteGranted = true;
                }
                server.ReceiveMessage(message, instance);
            }
        }
    }

    // Act
    messagesSent.clear();
    mockTimeKeeper->currentTime += configuration.rpcTimeout - 0.0001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(0, messagesSent.size());
}

TEST_F(ServerTests, ServerRegularRetransmissions) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    configuration.rpcTimeout = 0.01;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    (void)AwaitServerMessagesToBeSent(4);
    for (auto instance: configuration.instanceNumbers) {
        switch (instance) {
            case 6:
            case 7:
            case 11: {
                const auto message = Raft::Message::CreateMessage();
                message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
                if (instance == 11) {
                    message->impl_->requestVoteResults.term = 1;
                    message->impl_->requestVoteResults.voteGranted = false;
                } else {
                    message->impl_->requestVoteResults.term = 0;
                    message->impl_->requestVoteResults.voteGranted = true;
                }
                server.ReceiveMessage(message, instance);
            }
        }
    }

    // Act
    messagesSent.clear();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout + configuration.rpcTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    EXPECT_EQ(1, messagesSent.size());
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout + configuration.rpcTimeout * 2 - 0.001;
    server.WaitForAtLeastOneWorkerLoop();
    EXPECT_EQ(1, messagesSent.size());
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout + configuration.rpcTimeout * 2 + 0.001;
    server.WaitForAtLeastOneWorkerLoop();
    EXPECT_EQ(2, messagesSent.size());
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout + configuration.rpcTimeout * 3 - 0.001;
    server.WaitForAtLeastOneWorkerLoop();
    EXPECT_EQ(2, messagesSent.size());
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout + configuration.rpcTimeout * 3 + 0.003;
    server.WaitForAtLeastOneWorkerLoop();
    EXPECT_EQ(3, messagesSent.size());

    // Assert
    for (const auto& messageSent: messagesSent) {
        EXPECT_EQ(2, messageSent.receiverInstanceNumber);
        EXPECT_EQ(
            Raft::MessageImpl::Type::RequestVote,
            messageSent.message->impl_->type
        );
        EXPECT_EQ(
            configuration.selfInstanceNumber,
            messageSent.message->impl_->requestVote.candidateId
        );
        EXPECT_EQ(
            1,
            messageSent.message->impl_->requestVote.term
        );
    }
}

TEST_F(ServerTests, ServerDoesNotReceiveMajorityVoteInElection) {
}

TEST_F(ServerTests, TimeoutBeforeAllVotesReceivedInElection) {
}

TEST_F(ServerTests, ReceiveElectionRequestWhenAlreadyVoted) {
}

TEST_F(ServerTests, ReceiveElectionRequestWhenNoVotePending) {
}
