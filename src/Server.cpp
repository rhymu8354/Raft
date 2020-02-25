/**
 * @file Server.cpp
 *
 * This module contains the implementation of the Raft::Server class.
 *
 * © 2018-2020 by Richard Walters
 */

#include "Message.hpp"
#include "ServerImpl.hpp"
#include "Utilities.hpp"

#include <algorithm>
#include <future>
#include <map>
#include <mutex>
#include <queue>
#include <Raft/LogEntry.hpp>
#include <Raft/ILog.hpp>
#include <Raft/Server.hpp>
#include <random>
#include <sstream>
#include <SystemAbstractions/CryptoRandom.hpp>
#include <SystemAbstractions/DiagnosticsSender.hpp>
#include <thread>
#include <time.h>

namespace Raft {

    Server::~Server() noexcept = default;
    Server::Server(Server&&) noexcept = default;
    Server& Server::operator=(Server&&) noexcept = default;

    Server::Server()
        : impl_(new Impl())
    {
        SystemAbstractions::CryptoRandom jim;
        int seed;
        jim.Generate(&seed, sizeof(seed));
        impl_->rng.seed(seed);
    }

    SystemAbstractions::DiagnosticsSender::UnsubscribeDelegate Server::SubscribeToDiagnostics(
        SystemAbstractions::DiagnosticsSender::DiagnosticMessageDelegate delegate,
        size_t minLevel
    ) {
        return impl_->diagnosticsSender.SubscribeToDiagnostics(delegate, minLevel);
    }

    size_t Server::GetCommitIndex() const {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        return impl_->commitIndex;
    }

    void Server::SetCommitIndex(size_t commitIndex) {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        impl_->commitIndex = commitIndex;
        for (auto& instanceEntry: impl_->instances) {
            instanceEntry.second.matchIndex = commitIndex;
            instanceEntry.second.nextIndex = commitIndex + 1;
        }
    }

    size_t Server::GetLastIndex() const {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        return impl_->lastIndex;
    }

    void Server::SetLastIndex(size_t lastIndex) {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        impl_->lastIndex = lastIndex;
    }

    size_t Server::GetNextIndex(int instanceId) const {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        return impl_->instances[instanceId].nextIndex;
    }

    size_t Server::GetMatchIndex(int instanceId) const {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        return impl_->instances[instanceId].matchIndex;
    }

    bool Server::IsVotingMember() const {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        return impl_->isVotingMember;
    }

    bool Server::HasJointConfiguration() const {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        return (impl_->jointConfiguration != nullptr);
    }

    int Server::GetClusterLeaderId() const {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        if (impl_->thisTermLeaderAnnounced) {
            return impl_->leaderId;
        } else {
            return 0;
        }
    }

    void Server::SetOnReceiveMessageCallback(std::function< void() > callback) {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        impl_->onReceiveMessageCallback = callback;
    }

    auto Server::SubscribeToEvents(EventDelegate eventDelegate) -> EventsUnsubscribeDelegate {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        const auto eventSubscriberId = impl_->nextEventSubscriberId++;
        impl_->eventSubscribers[eventSubscriberId] = eventDelegate;
        const std::weak_ptr< Impl > implWeak = impl_;
        return [implWeak, eventSubscriberId]{
            const auto impl = implWeak.lock();
            if (impl == nullptr) {
                return;
            }
            std::lock_guard< decltype(impl->mutex) > lock(impl->mutex);
            impl->eventSubscribers.erase(eventSubscriberId);
        };
    }

    void Server::Mobilize(
        std::shared_ptr< ILog > logKeeper,
        std::shared_ptr< IPersistentState > persistentStateKeeper,
        std::shared_ptr< Timekeeping::Scheduler > scheduler,
        const ClusterConfiguration& clusterConfiguration,
        const ServerConfiguration& serverConfiguration
    ) {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        if (impl_->mobilized) {
            return;
        }
        ++impl_->generation;
        impl_->mobilized = true;
        impl_->logKeeper = logKeeper;
        impl_->persistentStateKeeper = persistentStateKeeper;
        impl_->scheduler = scheduler;
        impl_->serverConfiguration = serverConfiguration;
        impl_->persistentStateCache = persistentStateKeeper->Load();
        impl_->instances.clear();
        impl_->electionState = IServer::ElectionState::Follower;
        impl_->thisTermLeaderAnnounced = false;
        impl_->votesForUsCurrentConfig = 0;
        impl_->ApplyConfiguration(clusterConfiguration);
        impl_->commitIndex = logKeeper->GetBaseIndex();
        impl_->lastIndex = 0;
        impl_->SetLastIndex(logKeeper->GetLastIndex());
        impl_->ResetStatistics();
        impl_->eventQueueWorker = std::thread(&Impl::EventQueueWorker, impl_.get());
        impl_->ResetElectionTimer();
    }

    void Server::Demobilize() {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        if (!impl_->mobilized) {
            return;
        }
        impl_->mobilized = false;
        if (impl_->heartbeatTimeoutToken != 0) {
            impl_->scheduler->Cancel(impl_->heartbeatTimeoutToken);
            impl_->heartbeatTimeoutToken = 0;
        }
        if (impl_->electionTimeoutToken != 0) {
            impl_->scheduler->Cancel(impl_->electionTimeoutToken);
            impl_->electionTimeoutToken = 0;
        }
        impl_->ResetRetransmissionState();
        impl_->scheduler = nullptr;
        if (impl_->eventQueueWorker.joinable()) {
            std::unique_lock< decltype(impl_->eventQueueMutex) > eventQueueLock(impl_->eventQueueMutex);
            impl_->stopEventQueueWorker = true;
            impl_->eventQueueWorkerWakeCondition.notify_one();
            eventQueueLock.unlock();
            impl_->eventQueueWorker.join();
        }
        impl_->persistentStateKeeper = nullptr;
        impl_->logKeeper = nullptr;
    }

    void Server::ReceiveMessage(
        const std::string& serializedMessage,
        int senderInstanceNumber
    ) {
        std::unique_lock< decltype(impl_->mutex) > lock(impl_->mutex);
        if (senderInstanceNumber == impl_->leaderId) {
            impl_->processingMessageFromLeader = true;
        }
        decltype(impl_->onReceiveMessageCallback) callback(impl_->onReceiveMessageCallback);
        lock.unlock();
        if (callback) {
            callback();
        }
        Message message(serializedMessage);
        lock.lock();
        switch (message.type) {
            case Message::Type::RequestVote: {
                impl_->OnReceiveRequestVote(
                    std::move(message),
                    senderInstanceNumber
                );
            } break;

            case Message::Type::RequestVoteResults: {
                impl_->OnReceiveRequestVoteResults(
                    std::move(message),
                    senderInstanceNumber
                );
            } break;

            case Message::Type::AppendEntries: {
                impl_->OnReceiveAppendEntries(
                    std::move(message),
                    senderInstanceNumber
                );
            } break;

            case Message::Type::AppendEntriesResults: {
                impl_->OnReceiveAppendEntriesResults(
                    std::move(message),
                    senderInstanceNumber
                );
            } break;

            case Message::Type::InstallSnapshot: {
                impl_->OnReceiveInstallSnapshot(
                    std::move(message),
                    senderInstanceNumber
                );
            } break;

            case Message::Type::InstallSnapshotResults: {
                impl_->OnReceiveInstallSnapshotResults(
                    std::move(message),
                    senderInstanceNumber
                );
            } break;

            default: {
            } break;
        }
        if (senderInstanceNumber == impl_->leaderId) {
            impl_->processingMessageFromLeader = false;
        }
    }

    auto Server::GetElectionState() -> ElectionState {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        return impl_->electionState;
    }

    void Server::AppendLogEntries(const std::vector< LogEntry >& entries) {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        if (impl_->electionState != ElectionState::Leader) {
            return;
        }
        impl_->AppendLogEntries(entries);
    }

    void Server::ChangeConfiguration(const ClusterConfiguration& newConfiguration) {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        if (impl_->electionState != ElectionState::Leader) {
            return;
        }
        impl_->diagnosticsSender.SendDiagnosticInformationFormatted(
            3,
            "ChangeConfiguration -- from %s to %s",
            FormatSet(impl_->clusterConfiguration.instanceIds).c_str(),
            FormatSet(newConfiguration.instanceIds).c_str()
        );
        impl_->configChangePending = true;
        impl_->newServerCatchUpIndex = impl_->commitIndex;
        impl_->ApplyConfiguration(
            impl_->clusterConfiguration,
            newConfiguration
        );
    }

    void Server::ResetStatistics() {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        impl_->ResetStatistics();
    }

    Json::Value Server::GetStatistics() {
        std::lock_guard< decltype(impl_->mutex) > lock(impl_->mutex);
        if (impl_->electionState == ElectionState::Leader) {
            return Json::Object({
                {"minBroadcastTime", (double)impl_->minBroadcastTime / 1000000.0},
                {
                    "avgBroadcastTime", (
                        (impl_->numBroadcastTimeMeasurements == 0)
                        ? 0.0
                        : (double)impl_->broadcastTimeMeasurementsSum / (double)impl_->numBroadcastTimeMeasurements / 1000000.0
                    )
                },
                {"maxBroadcastTime", (double)impl_->maxBroadcastTime / 1000000.0},
            });
        } else {
            const auto now = impl_->scheduler->GetClock()->GetCurrentTime();
            return Json::Object({
                {"minTimeBetweenLeaderMessages", (double)impl_->minTimeBetweenLeaderMessages / 1000000.0},
                {
                    "avgTimeBetweenLeaderMessages", (
                        (impl_->numTimeBetweenLeaderMessagesMeasurements == 0)
                        ? 0.0
                        : (double)impl_->timeBetweenLeaderMessagesMeasurementsSum / (double)impl_->numTimeBetweenLeaderMessagesMeasurements / 1000000.0
                    )
                },
                {"maxTimeBetweenLeaderMessages", (double)impl_->maxTimeBetweenLeaderMessages / 1000000.0},
                {"timeSinceLastLeaderMessage", now - impl_->timeLastMessageReceivedFromLeader},
            });
        }
    }

}
