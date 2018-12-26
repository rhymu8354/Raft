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
        int receiverInstanceNumber;
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
    Raft::Server::Configuration configuration;
    std::vector< std::string > diagnosticMessages;
    SystemAbstractions::DiagnosticsSender::UnsubscribeDelegate diagnosticsUnsubscribeDelegate;
    const std::shared_ptr< MockTimeKeeper > mockTimeKeeper = std::make_shared< MockTimeKeeper >();
    std::vector< MessageInfo > messagesSent;

    // Methods

    void ServerSentMessage(
        std::shared_ptr< Raft::Message > message,
        int receiverInstanceNumber
    ) {
        MessageInfo messageInfo;
        messageInfo.message = message;
        messageInfo.receiverInstanceNumber = receiverInstanceNumber;
        messagesSent.push_back(std::move(messageInfo));
    }

    void BecomeLeader() {
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
    }

    void BecomeFollower(
        int leaderId,
        int term
    ) {
        server.Configure(configuration);
        server.Mobilize();
        server.WaitForAtLeastOneWorkerLoop();
        const auto message = std::make_shared< Raft::Message >();
        message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
        message->impl_->appendEntries.term = term;
        server.ReceiveMessage(message, leaderId);
        server.WaitForAtLeastOneWorkerLoop();
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
                int receiverInstanceNumber
            ){
                ServerSentMessage(message, receiverInstanceNumber);
            }
        );
        configuration.instanceNumbers = {2, 5, 6, 7, 11};
        configuration.selfInstanceNumber = 5;
        configuration.minimumElectionTimeout = 0.1;
        configuration.maximumElectionTimeout = 0.2;
        configuration.rpcTimeout = 0.01;
        configuration.currentTerm = 0;
    }

    virtual void TearDown() {
        server.Demobilize();
        diagnosticsUnsubscribeDelegate();
    }
};

TEST_F(ServerTests, InitialConfiguration) {
    // Arrange
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
    server.Configure(configuration);
    server.Mobilize();

    // Act
    server.Mobilize();
}

TEST_F(ServerTests, ElectionNeverStartsBeforeMinimumTimeoutInterval) {
    // Arrange
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
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Candidate,
        server.GetElectionState()
    );
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
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(4, messagesSent.size());
    std::set< int > instances(
        configuration.instanceNumbers.begin(),
        configuration.instanceNumbers.end()
    );
    for (const auto messageInfo: messagesSent) {
        (void)instances.erase(messageInfo.receiverInstanceNumber);
    }
    EXPECT_EQ(
        std::set< int >{ configuration.selfInstanceNumber },
        instances
    );
}

TEST_F(ServerTests, ServerVotesForItselfInElectionItStarts) {
    // Arrange
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
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Leader,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, ServerDoesReceiveNonUnanimousMajorityVoteInElection) {
    // Arrange
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
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Leader,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, ServerRetransmitsRequestVoteForSlowVotersInElection) {
    // Arrange
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
    server.WaitForAtLeastOneWorkerLoop();

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
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    messagesSent.clear();
    mockTimeKeeper->currentTime += configuration.rpcTimeout - 0.0001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(0, messagesSent.size());
}

TEST_F(ServerTests, ServerRegularRetransmissions) {
    // Arrange
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
    server.WaitForAtLeastOneWorkerLoop();

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
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Candidate,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, ServerAlmostWinsElection) {
    // Arrange
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
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Candidate,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, TimeoutBeforeMajorityVoteOrNewLeaderHeartbeat) {
    // Arrange
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
    server.WaitForAtLeastOneWorkerLoop();
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

TEST_F(ServerTests, ReceiveVoteRequestLesserTerm) {
    // Arrange
    configuration.currentTerm = 1;
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
    EXPECT_EQ(1, messagesSent[0].message->impl_->requestVoteResults.term);
    EXPECT_FALSE(messagesSent[0].message->impl_->requestVoteResults.voteGranted);
}

TEST_F(ServerTests, ReceiveVoteRequestGreaterTermWhenFollower) {
    // Arrange
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
    server.WaitForAtLeastOneWorkerLoop();
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
    EXPECT_EQ(
        Raft::IServer::ElectionState::Follower,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, ReceiveVoteRequestGreaterTermWhenLeader) {
    // Arrange
    BecomeLeader();

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
    EXPECT_EQ(
        Raft::IServer::ElectionState::Follower,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, DoNotStartVoteWhenAlreadyLeader) {
    // Arrange
    BecomeLeader();

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
    BecomeLeader();
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
    BecomeLeader();

    // Act
    const auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
    message->impl_->appendEntries.term = 2;
    server.ReceiveMessage(message, 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(0, messagesSent.size());
    EXPECT_EQ(
        Raft::IServer::ElectionState::Follower,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, CandidateShouldRevertToFollowerWhenGreaterTermHeartbeatReceived) {
    // Arrange
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();

    // Act
    const auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
    message->impl_->appendEntries.term = 2;
    server.ReceiveMessage(message, 2);
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
    mockTimeKeeper->currentTime += configuration.minimumElectionTimeout - 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(0, messagesSent.size());
    EXPECT_EQ(
        Raft::IServer::ElectionState::Follower,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, CandidateShouldRevertToFollowerWhenSameTermHeartbeatReceived) {
    // Arrange
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();

    // Act
    const auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
    message->impl_->appendEntries.term = 1;
    server.ReceiveMessage(message, 2);
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
    mockTimeKeeper->currentTime += configuration.minimumElectionTimeout - 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(0, messagesSent.size());
    EXPECT_EQ(
        Raft::IServer::ElectionState::Follower,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, LeaderShouldSendRegularHeartbeats) {
    // Arrange
    BecomeLeader();

    // Act
    mockTimeKeeper->currentTime += configuration.minimumElectionTimeout / 2 + 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    std::map< int, size_t > heartbeatsReceivedPerInstance;
    for (auto instanceNumber: configuration.instanceNumbers) {
        heartbeatsReceivedPerInstance[instanceNumber] = 0;
    }
    for (const auto& messageSent: messagesSent) {
        if (messageSent.message->impl_->type == Raft::MessageImpl::Type::AppendEntries) {
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

TEST_F(ServerTests, RepeatVotesShouldNotCount) {
    // Arrange
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
            if (instance == 2) {
                message->impl_->requestVoteResults.voteGranted = true;
            } else {
                message->impl_->requestVoteResults.voteGranted = false;
            }
            server.ReceiveMessage(message, instance);
        }
    }
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    const auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
    message->impl_->requestVoteResults.term = 1;
    message->impl_->requestVoteResults.voteGranted = true;
    server.ReceiveMessage(message, 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(
        Raft::IServer::ElectionState::Candidate,
        server.GetElectionState()
    );
}

TEST_F(ServerTests, UpdateTermWhenReceivingHeartBeat) {
    // Arrange
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    const auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
    message->impl_->appendEntries.term = 2;
    server.ReceiveMessage(message, 2);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(2, server.GetConfiguration().currentTerm);
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
    configuration.currentTerm = newTerm;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    const auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
    message->impl_->appendEntries.term = newTerm;
    server.ReceiveMessage(message, leaderId);
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
    configuration.currentTerm = newTerm;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
    message->impl_->appendEntries.term = newTerm;
    server.ReceiveMessage(message, leaderId);
    server.WaitForAtLeastOneWorkerLoop();
    leadershipChangeAnnounced = false;

    // Act
    message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
    message->impl_->appendEntries.term = newTerm;
    server.ReceiveMessage(message, leaderId);
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
    configuration.currentTerm = 0;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    const auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
    message->impl_->appendEntries.term = newTerm;
    server.ReceiveMessage(message, leaderId);
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
    configuration.currentTerm = 0;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
    message->impl_->appendEntries.term = newTerm;
    server.ReceiveMessage(message, leaderId);
    server.WaitForAtLeastOneWorkerLoop();
    leadershipChangeAnnounced = false;

    // Act
    message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
    message->impl_->appendEntries.term = newTerm;
    server.ReceiveMessage(message, leaderId);
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
    configuration.currentTerm = 0;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
    message->impl_->appendEntries.term = firstTerm;
    server.ReceiveMessage(message, firstLeaderId);
    server.WaitForAtLeastOneWorkerLoop();
    leadershipChangeAnnounced = false;

    // Act
    message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
    message->impl_->appendEntries.term = secondTerm;
    server.ReceiveMessage(message, secondLeaderId);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_TRUE(leadershipChangeAnnounced);
    EXPECT_EQ(secondLeaderId, leadershipChangeDetails.leaderId);
    EXPECT_EQ(secondTerm, leadershipChangeDetails.term);
}

TEST_F(ServerTests, ReceivingHeartBeatFromSameTermShouldResetElectionTimeout) {
    // Arrange
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    while (mockTimeKeeper->currentTime <= configuration.maximumElectionTimeout * 2) {
        const auto message = std::make_shared< Raft::Message >();
        message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
        message->impl_->appendEntries.term = 2;
        server.ReceiveMessage(message, 2);
        mockTimeKeeper->currentTime += 0.001;
        server.WaitForAtLeastOneWorkerLoop();
        ASSERT_TRUE(messagesSent.empty());
    }

    // Assert
}

TEST_F(ServerTests, IgnoreHeartBeatFromOldTerm) {
    // Arrange
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
    message->impl_->appendEntries.term = 2;
    server.ReceiveMessage(message, 2);

    // Act
    message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
    message->impl_->appendEntries.term = 1;
    server.ReceiveMessage(message, 2);

    // Assert
    EXPECT_EQ(2, server.GetConfiguration().currentTerm);
}

TEST_F(ServerTests, ReceivingHeartBeatFromOldTermShouldNotResetElectionTimeout) {
    // Arrange
    configuration.currentTerm = 42;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();

    // Act
    bool electionStarted = false;
    while (mockTimeKeeper->currentTime <= configuration.maximumElectionTimeout * 2) {
        const auto message = std::make_shared< Raft::Message >();
        message->impl_->type = Raft::MessageImpl::Type::AppendEntries;
        message->impl_->appendEntries.term = 13;
        server.ReceiveMessage(message, 2);
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
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();

    // Act
    const auto message = std::make_shared< Raft::Message >();
    message->impl_->type = Raft::MessageImpl::Type::RequestVote;
    message->impl_->requestVote.term = 3;
    message->impl_->requestVote.candidateId = 2;
    server.ReceiveMessage(message, 2);
    server.WaitForAtLeastOneWorkerLoop();
    messagesSent.clear();
    mockTimeKeeper->currentTime += configuration.minimumElectionTimeout - 0.001;
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    EXPECT_EQ(0, messagesSent.size());
    EXPECT_EQ(
        Raft::IServer::ElectionState::Follower,
        server.GetElectionState()
    );
    EXPECT_EQ(3, server.GetConfiguration().currentTerm);
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
    configuration.currentTerm = 0;
    configuration.selfInstanceNumber = 5;
    BecomeLeader();
    server.WaitForAtLeastOneWorkerLoop();

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
    configuration.currentTerm = 0;
    configuration.selfInstanceNumber = 5;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    auto instanceNumbersEntry = configuration.instanceNumbers.begin();
    size_t votesGranted = 0;
    do {
        const auto instance = *instanceNumbersEntry++;
        if (instance == configuration.selfInstanceNumber) {
            continue;
        }
        const auto message = std::make_shared< Raft::Message >();
        message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
        message->impl_->requestVoteResults.term = 1;
        message->impl_->requestVoteResults.voteGranted = true;
        server.ReceiveMessage(message, instance);
        server.WaitForAtLeastOneWorkerLoop();
        EXPECT_FALSE(leadershipChangeAnnounced) << votesGranted;
        ++votesGranted;
    } while (votesGranted + 1 < configuration.instanceNumbers.size() / 2);

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
    configuration.currentTerm = 0;
    configuration.selfInstanceNumber = 5;
    server.Configure(configuration);
    server.Mobilize();
    server.WaitForAtLeastOneWorkerLoop();
    mockTimeKeeper->currentTime = configuration.maximumElectionTimeout;
    server.WaitForAtLeastOneWorkerLoop();
    for (auto instance: configuration.instanceNumbers) {
        if (instance == configuration.selfInstanceNumber) {
            continue;
        }
        const auto wasLeader = (
            server.GetElectionState() == Raft::IServer::ElectionState::Leader
        );
        const auto message = std::make_shared< Raft::Message >();
        message->impl_->type = Raft::MessageImpl::Type::RequestVoteResults;
        message->impl_->requestVoteResults.term = 1;
        message->impl_->requestVoteResults.voteGranted = true;
        server.ReceiveMessage(message, instance);
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
    configuration.currentTerm = 0;
    configuration.selfInstanceNumber = 5;
    BecomeFollower(leaderId, newTerm);
    server.WaitForAtLeastOneWorkerLoop();

    // Assert
    ASSERT_TRUE(leadershipChangeAnnounced);
    EXPECT_EQ(leaderId, leadershipChangeDetails.leaderId);
    EXPECT_EQ(newTerm, leadershipChangeDetails.term);
}
