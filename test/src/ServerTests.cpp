/**
 * @file ServerTests.cpp
 *
 * This module contains the unit tests of the
 * Raft::Server class.
 *
 * © 2018 by Richard Walters
 */

#include <src/MessageImpl.hpp>

#include <algorithm>
#include <condition_variable>
#include <gtest/gtest.h>
#include <limits>
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

    // Methods

    void ServerSentMessage(
        std::shared_ptr< Raft::Message > message,
        unsigned int receiverInstanceNumber
    ) {
        MessageInfo messageInfo;
        messageInfo.message = message;
        messageInfo.receiverInstanceNumber = receiverInstanceNumber;
        messagesSent.push_back(std::move(messageInfo));
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
        server.SetCreateMessageDelegate([]{ return std::make_shared< Raft::Message >(); });
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

TEST_F(ServerTests, MobilizeTwiceDoesNotCrash) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    server.Configure(configuration);
    server.Mobilize();

    // Act
    server.Mobilize();
}

TEST_F(ServerTests, ElectionNeverStartsBeforeMinimumTimeoutInterval) {
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
    mockTimeKeeper->currentTime = configuration.minimumElectionTimeout - 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(0, messagesSent.size());
}

TEST_F(ServerTests, ElectionAlwaysStartedWithinMaximumTimeoutInterval) {
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
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(4, messagesSent.size());
    for (const auto messageInfo: messagesSent) {
        EXPECT_EQ(
            Raft::MessageImpl::Type::RequestVote,
            messageInfo.message->impl_->type
        );
        EXPECT_EQ(1, messageInfo.message->impl_->requestVote.term);
    }
}

TEST_F(ServerTests, ElectionStartsAfterRandomIntervalBetweenMinimumAndMaximum) {
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
    const auto binInterval = 0.001;
    std::vector< size_t > bins(
        (size_t)(
            (configuration.maximumElectionTimeout - configuration.minimumElectionTimeout)
            / binInterval
        )
    );
    for (size_t i = 0; i < 100; ++i) {
        messagesSent.clear();
        mockTimeKeeper->currentTime = 0.0;
        server.Demobilize();
        server.Mobilize();
        server.WaitForAtLeastOneWorkerLoop();
        mockTimeKeeper->currentTime = configuration.minimumElectionTimeout + binInterval;
        for (
            size_t j = 0;
            mockTimeKeeper->currentTime <= configuration.maximumElectionTimeout;
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
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
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
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(4, messagesSent.size());
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
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(4, messagesSent.size());
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
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    for (auto instance: configuration.instanceNumbers) {
        if (instance != configuration.selfInstanceNumber) {
            const auto message = std::make_shared< Raft::Message >();
            message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
            message->impl_->requestVoteResults.term = 1;
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
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    for (auto instance: configuration.instanceNumbers) {
        if (instance != configuration.selfInstanceNumber) {
            const auto message = std::make_shared< Raft::Message >();
            message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
            if (instance == 11) {
                message->impl_->requestVoteResults.term = 1;
                message->impl_->requestVoteResults.voteGranted = false;
            } else {
                message->impl_->requestVoteResults.term = 1;
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
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: configuration.instanceNumbers) {
        switch (instance) {
            case 6:
            case 7:
            case 11: {
                const auto message = std::make_shared< Raft::Message >();
                message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
                if (instance == 11) {
                    message->impl_->requestVoteResults.term = 1;
                    message->impl_->requestVoteResults.voteGranted = false;
                } else {
                    message->impl_->requestVoteResults.term = 1;
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
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: configuration.instanceNumbers) {
        switch (instance) {
            case 6:
            case 7:
            case 11: {
                const auto message = std::make_shared< Raft::Message >();
                message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
                if (instance == 11) {
                    message->impl_->requestVoteResults.term = 1;
                    message->impl_->requestVoteResults.voteGranted = false;
                } else {
                    message->impl_->requestVoteResults.term = 1;
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
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: configuration.instanceNumbers) {
        switch (instance) {
            case 6:
            case 7:
            case 11: {
                const auto message = std::make_shared< Raft::Message >();
                message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
                if (instance == 11) {
                    message->impl_->requestVoteResults.term = 1;
                    message->impl_->requestVoteResults.voteGranted = false;
                } else {
                    message->impl_->requestVoteResults.term = 1;
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

TEST_F(ServerTests, ServerDoesNotReceiveAnyVotesInElection) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    for (auto instance: configuration.instanceNumbers) {
        if (instance != configuration.selfInstanceNumber) {
            const auto message = std::make_shared< Raft::Message >();
            message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
            message->impl_->requestVoteResults.term = 1;
            message->impl_->requestVoteResults.voteGranted = false;
            server.ReceiveMessage(message, instance);
        }
    }

    // Assert
    EXPECT_FALSE(server.IsLeader());
}

TEST_F(ServerTests, ServerAlmostWinsElection) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    for (auto instance: configuration.instanceNumbers) {
        if (instance != configuration.selfInstanceNumber) {
            const auto message = std::make_shared< Raft::Message >();
            message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
            if (
                (instance == 2)
                || (instance == 6)
                || (instance == 11)
            ) {
                message->impl_->requestVoteResults.term = 1;
                message->impl_->requestVoteResults.voteGranted = false;
            } else {
                message->impl_->requestVoteResults.term = 1;
                message->impl_->requestVoteResults.voteGranted = true;
            }
            server.ReceiveMessage(message, instance);
        }
    }

    // Assert
    EXPECT_FALSE(server.IsLeader());
}

TEST_F(ServerTests, TimeoutBeforeMajorityVoteOrNewLeaderHeartbeat) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: configuration.instanceNumbers) {
        if (instance != configuration.selfInstanceNumber) {
            const auto message = std::make_shared< Raft::Message >();
            message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
            message->impl_->requestVoteResults.term = 1;
            message->impl_->requestVoteResults.voteGranted = false;
            server.ReceiveMessage(message, instance);
        }
    }
    messagesSent.clear();

    // Act
    mockTimeKeeper->currentTime += configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    for (const auto messageInfo: messagesSent) {
        EXPECT_EQ(
            Raft::MessageImpl::Type::RequestVote,
            messageInfo.message->impl_->type
        );
        EXPECT_EQ(2, messageInfo.message->impl_->requestVote.term);
    }
}

TEST_F(ServerTests, ReceiveVoteRequestWhenSameTermNoVotePending) {
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
    const auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::RequestVote;
    message->impl_->requestVote.term = 0;
    message->impl_->requestVote.candidateId = 2;
    server.ReceiveMessage(message, 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(
        Raft::MessageImpl::Type::RequestVoteResults,
        messagesSent[0].message->impl_->type
    );
    EXPECT_EQ(0, messagesSent[0].message->impl_->requestVoteResults.term);
    EXPECT_TRUE(messagesSent[0].message->impl_->requestVoteResults.voteGranted);
}

TEST_F(ServerTests, ReceiveVoteRequestWhenSameTermAlreadyVotedForAnother) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::RequestVote;
    message->impl_->requestVote.term = 1;
    message->impl_->requestVote.candidateId = 2;
    server.ReceiveMessage(message, 2);
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();

    // Act
    message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::RequestVote;
    message->impl_->requestVote.term = 1;
    message->impl_->requestVote.candidateId = 6;
    server.ReceiveMessage(message, 6);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(
        Raft::MessageImpl::Type::RequestVoteResults,
        messagesSent[0].message->impl_->type
    );
    EXPECT_EQ(1, messagesSent[0].message->impl_->requestVoteResults.term);
    EXPECT_FALSE(messagesSent[0].message->impl_->requestVoteResults.voteGranted);
}

TEST_F(ServerTests, ReceiveVoteRequestWhenSameTermAlreadyVotedForSame) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::RequestVote;
    message->impl_->requestVote.term = 1;
    message->impl_->requestVote.candidateId = 2;
    server.ReceiveMessage(message, 2);
    messagesSent.clear();

    // Act
    message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::RequestVote;
    message->impl_->requestVote.term = 1;
    message->impl_->requestVote.candidateId = 2;
    server.ReceiveMessage(message, 2);

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(
        Raft::MessageImpl::Type::RequestVoteResults,
        messagesSent[0].message->impl_->type
    );
    EXPECT_EQ(1, messagesSent[0].message->impl_->requestVoteResults.term);
    EXPECT_TRUE(messagesSent[0].message->impl_->requestVoteResults.voteGranted);
}

TEST_F(ServerTests, ReceiveVoteRequestLesserTerm) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: configuration.instanceNumbers) {
        if (instance != configuration.selfInstanceNumber) {
            const auto message = std::make_shared< Raft::Message >();
            message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
            message->impl_->requestVoteResults.term = 1;
            message->impl_->requestVoteResults.voteGranted = true;
            server.ReceiveMessage(message, instance);
        }
    }
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();

    // Act
    const auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::RequestVote;
    message->impl_->requestVote.term = 0;
    message->impl_->requestVote.candidateId = 2;
    server.ReceiveMessage(message, 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(
        Raft::MessageImpl::Type::RequestVoteResults,
        messagesSent[0].message->impl_->type
    );
    EXPECT_EQ(1, messagesSent[0].message->impl_->requestVoteResults.term);
    EXPECT_FALSE(messagesSent[0].message->impl_->requestVoteResults.voteGranted);
}

TEST_F(ServerTests, ReceiveVoteRequestGreaterTermWhenFollower) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    const auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::RequestVote;
    message->impl_->requestVote.term = 1;
    message->impl_->requestVote.candidateId = 2;
    server.ReceiveMessage(message, 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(
        Raft::MessageImpl::Type::RequestVoteResults,
        messagesSent[0].message->impl_->type
    );
    EXPECT_EQ(1, messagesSent[0].message->impl_->requestVoteResults.term);
    EXPECT_TRUE(messagesSent[0].message->impl_->requestVoteResults.voteGranted);
}

TEST_F(ServerTests, ReceiveVoteRequestGreaterTermWhenCandidate) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();

    // Act
    const auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::RequestVote;
    message->impl_->requestVote.term = 2;
    message->impl_->requestVote.candidateId = 2;
    server.ReceiveMessage(message, 2);
    mockTimeKeeper->currentTime += configuration.minimumElectionTimeout - 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(
        Raft::MessageImpl::Type::RequestVoteResults,
        messagesSent[0].message->impl_->type
    );
    EXPECT_EQ(2, messagesSent[0].message->impl_->requestVoteResults.term);
    EXPECT_TRUE(messagesSent[0].message->impl_->requestVoteResults.voteGranted);
    EXPECT_FALSE(server.IsLeader());
}

TEST_F(ServerTests, ReceiveVoteRequestGreaterTermWhenLeader) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: configuration.instanceNumbers) {
        if (instance != configuration.selfInstanceNumber) {
            const auto message = std::make_shared< Raft::Message >();
            message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
            message->impl_->requestVoteResults.term = 1;
            message->impl_->requestVoteResults.voteGranted = true;
            server.ReceiveMessage(message, instance);
        }
    }
    messagesSent.clear();

    // Act
    const auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::RequestVote;
    message->impl_->requestVote.term = 2;
    message->impl_->requestVote.candidateId = 2;
    server.ReceiveMessage(message, 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_EQ(1, messagesSent.size());
    EXPECT_EQ(
        Raft::MessageImpl::Type::RequestVoteResults,
        messagesSent[0].message->impl_->type
    );
    EXPECT_EQ(2, messagesSent[0].message->impl_->requestVoteResults.term);
    EXPECT_TRUE(messagesSent[0].message->impl_->requestVoteResults.voteGranted);
    EXPECT_FALSE(server.IsLeader());
}

TEST_F(ServerTests, DoNotStartVoteWhenAlreadyLeader) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: configuration.instanceNumbers) {
        if (instance != configuration.selfInstanceNumber) {
            const auto message = std::make_shared< Raft::Message >();
            message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
            message->impl_->requestVoteResults.term = 1;
            message->impl_->requestVoteResults.voteGranted = true;
            server.ReceiveMessage(message, instance);
        }
    }
    messagesSent.clear();

    // Arrange
    mockTimeKeeper->currentTime += configuration.maximumElectionTimeout + 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    for (const auto& messageSent: messagesSent) {
        EXPECT_NE(
            Raft::MessageImpl::Type::RequestVote,
            messageSent.message->impl_->type
        );
    }
}

TEST_F(ServerTests, AfterRevertToFollowerDoNotStartNewElectionBeforeMinimumTimeout) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: configuration.instanceNumbers) {
        if (instance != configuration.selfInstanceNumber) {
            const auto message = std::make_shared< Raft::Message >();
            message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
            message->impl_->requestVoteResults.term = 1;
            message->impl_->requestVoteResults.voteGranted = true;
            server.ReceiveMessage(message, instance);
        }
    }
    mockTimeKeeper->currentTime += configuration.maximumElectionTimeout * 5;
    messagesSent.clear();
    const auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::RequestVote;
    message->impl_->requestVote.term = 2;
    message->impl_->requestVote.candidateId = 2;
    server.ReceiveMessage(message, 2);
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();

    // Act
    mockTimeKeeper->currentTime += configuration.minimumElectionTimeout - 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(0, messagesSent.size());
}

TEST_F(ServerTests, LeaderShouldRevertToFollowerWhenGreaterTermHeartbeatReceived) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: configuration.instanceNumbers) {
        if (instance != configuration.selfInstanceNumber) {
            const auto message = std::make_shared< Raft::Message >();
            message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
            message->impl_->requestVoteResults.term = 1;
            message->impl_->requestVoteResults.voteGranted = true;
            server.ReceiveMessage(message, instance);
        }
    }
    messagesSent.clear();

    // Act
    const auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::HeartBeat;
    message->impl_->heartbeat.term = 2;
    server.ReceiveMessage(message, 2);

    // Assert
    EXPECT_EQ(0, messagesSent.size());
    EXPECT_FALSE(server.IsLeader());
}

TEST_F(ServerTests, CandidateShouldRevertToFollowerWhenSameOrGreaterTermHeartbeatReceived) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();

    // Act
    const auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::HeartBeat;
    message->impl_->heartbeat.term = 2;
    server.ReceiveMessage(message, 2);
    mockTimeKeeper->currentTime += configuration.minimumElectionTimeout - 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(0, messagesSent.size());
    EXPECT_FALSE(server.IsLeader());
}

TEST_F(ServerTests, LeaderShouldSendRegularHeartbeats) {
    // Arrange
    Raft::Server::Configuration configuration;
    configuration.instanceNumbers = {2, 5, 6, 7, 11};
    configuration.selfInstanceNumber = 5;
    configuration.minimumElectionTimeout = 0.1;
    configuration.maximumElectionTimeout = 0.2;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: configuration.instanceNumbers) {
        if (instance != configuration.selfInstanceNumber) {
            const auto message = std::make_shared< Raft::Message >();
            message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
            message->impl_->requestVoteResults.term = 1;
            message->impl_->requestVoteResults.voteGranted = true;
            server.ReceiveMessage(message, instance);
        }
    }
    messagesSent.clear();

    // Act
    mockTimeKeeper->currentTime += configuration.minimumElectionTimeout / 2 + 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    std::map< unsigned int, size_t > heartbeatsReceivedPerInstance;
    for (auto instanceNumber: configuration.instanceNumbers) {
        heartbeatsReceivedPerInstance[instanceNumber] = 0;
    }
    for (const auto& messageSent: messagesSent) {
        if (messageSent.message->impl_->type == Raft::MessageImpl::Type::HeartBeat) {
            ++heartbeatsReceivedPerInstance[messageSent.receiverInstanceNumber];
        }
    }
    for (auto instanceNumber: configuration.instanceNumbers) {
        if (instanceNumber == configuration.selfInstanceNumber) {
            EXPECT_EQ(0, heartbeatsReceivedPerInstance[instanceNumber]);
        } else {
            EXPECT_NE(0, heartbeatsReceivedPerInstance[instanceNumber]);
        }
    }
}
