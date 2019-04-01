/**
 * @file Server.cpp
 *
 * This module contains the implementation of the Raft::Server class.
 *
 * © 2018 by Richard Walters
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
#include <Raft/TimeKeeper.hpp>
#include <random>
#include <sstream>
#include <SystemAbstractions/CryptoRandom.hpp>
#include <SystemAbstractions/DiagnosticsSender.hpp>
#include <thread>
#include <time.h>

namespace {

    /**
     * This is the maximum number of broadcast time samples to measure
     * before dropping old measurements.
     */
    constexpr size_t NUM_BROADCAST_TIME_SAMPLES = 1024;

}

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
        impl_->shared->rng.seed(seed);
    }

    SystemAbstractions::DiagnosticsSender::UnsubscribeDelegate Server::SubscribeToDiagnostics(
        SystemAbstractions::DiagnosticsSender::DiagnosticMessageDelegate delegate,
        size_t minLevel
    ) {
        return impl_->shared->diagnosticsSender.SubscribeToDiagnostics(delegate, minLevel);
    }

    void Server::SetTimeKeeper(std::shared_ptr< TimeKeeper > timeKeeper) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->timeKeeper = timeKeeper;
    }

    void Server::WaitForAtLeastOneWorkerLoop() {
        std::unique_lock< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->shared->workerLoopCompletion = std::make_shared< std::promise< void > >();
        auto workerLoopWasCompleted = impl_->shared->workerLoopCompletion->get_future();
        impl_->workerAskedToStopOrWakeUp.notify_one();
        lock.unlock();
        workerLoopWasCompleted.wait();
    }

    size_t Server::GetCommitIndex() const {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        return impl_->shared->commitIndex;
    }

    void Server::SetCommitIndex(size_t commitIndex) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->shared->commitIndex = commitIndex;
        for (auto& instanceEntry: impl_->shared->instances) {
            instanceEntry.second.matchIndex = commitIndex;
            instanceEntry.second.nextIndex = commitIndex + 1;
        }
    }

    size_t Server::GetLastIndex() const {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        return impl_->shared->lastIndex;
    }

    void Server::SetLastIndex(size_t lastIndex) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->shared->lastIndex = lastIndex;
    }

    size_t Server::GetNextIndex(int instanceId) const {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        return impl_->shared->instances[instanceId].nextIndex;
    }

    size_t Server::GetMatchIndex(int instanceId) const {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        return impl_->shared->instances[instanceId].matchIndex;
    }

    bool Server::IsVotingMember() const {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        return impl_->shared->isVotingMember;
    }

    bool Server::HasJointConfiguration() const {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        return (impl_->shared->jointConfiguration != nullptr);
    }

    int Server::GetClusterLeaderId() const {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        if (impl_->shared->thisTermLeaderAnnounced) {
            return impl_->shared->leaderId;
        } else {
            return 0;
        }
    }

    double Server::GetElectionTimeout() const {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        return impl_->shared->currentElectionTimeout;
    }

    void Server::SetSendMessageDelegate(SendMessageDelegate sendMessageDelegate) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->sendMessageDelegate = sendMessageDelegate;
    }

    void Server::SetLeadershipChangeDelegate(LeadershipChangeDelegate leadershipChangeDelegate) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->leadershipChangeDelegate = leadershipChangeDelegate;
    }

    void Server::SetElectionStateChangeDelegate(ElectionStateChangeDelegate electionStateChangeDelegate) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->electionStateChangeDelegate = electionStateChangeDelegate;
    }

    void Server::SetApplyConfigurationDelegate(ApplyConfigurationDelegate applyConfigurationDelegate) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->applyConfigurationDelegate = applyConfigurationDelegate;
    }

    void Server::SetCommitConfigurationDelegate(CommitConfigurationDelegate commitConfigurationDelegate) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->commitConfigurationDelegate = commitConfigurationDelegate;
    }

    void Server::SetCaughtUpDelegate(CaughtUpDelegate caughtUpDelegate) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->caughtUpDelegate = caughtUpDelegate;
        if (impl_->shared->caughtUp) {
            impl_->QueueCaughtUpAnnouncement();
        }
    }

    void Server::SetSnapshotInstalledDelegate(SnapshotInstalledDelegate snapshotInstalledDelegate) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->snapshotInstalledDelegate = snapshotInstalledDelegate;
    }

    void Server::Mobilize(
        std::shared_ptr< ILog > logKeeper,
        std::shared_ptr< IPersistentState > persistentStateKeeper,
        const ClusterConfiguration& clusterConfiguration,
        const ServerConfiguration& serverConfiguration
    ) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        if (impl_->worker.joinable()) {
            return;
        }
        impl_->shared->logKeeper = logKeeper;
        impl_->shared->persistentStateKeeper = persistentStateKeeper;
        impl_->shared->serverConfiguration = serverConfiguration;
        impl_->shared->persistentStateCache = persistentStateKeeper->Load();
        impl_->shared->instances.clear();
        impl_->shared->electionState = IServer::ElectionState::Follower;
        impl_->shared->timeOfLastLeaderMessage = 0.0;
        impl_->shared->thisTermLeaderAnnounced = false;
        impl_->shared->votesForUsCurrentConfig = 0;
        impl_->ApplyConfiguration(clusterConfiguration);
        impl_->shared->commitIndex = logKeeper->GetBaseIndex();
        impl_->shared->lastIndex = 0;
        impl_->SetLastIndex(logKeeper->GetLastIndex());
        ResetStatistics();
        impl_->stopWorker = std::promise< void >();
        impl_->worker = std::thread(&Impl::Worker, impl_.get());
    }

    void Server::Demobilize() {
        std::unique_lock< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        if (!impl_->worker.joinable()) {
            return;
        }
        impl_->stopWorker.set_value();
        impl_->workerAskedToStopOrWakeUp.notify_one();
        lock.unlock();
        impl_->worker.join();
        impl_->shared->persistentStateKeeper = nullptr;
        impl_->shared->logKeeper = nullptr;
    }

    void Server::ReceiveMessage(
        const std::string& serializedMessage,
        int senderInstanceNumber
    ) {
        Message message(serializedMessage);
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        switch (message.type) {
            case Message::Type::RequestVote: {
                impl_->OnReceiveRequestVote(message.requestVote, senderInstanceNumber);
            } break;

            case Message::Type::RequestVoteResults: {
                impl_->OnReceiveRequestVoteResults(message.requestVoteResults, senderInstanceNumber);
            } break;

            case Message::Type::AppendEntries: {
                impl_->OnReceiveAppendEntries(
                    message.appendEntries,
                    std::move(message.log),
                    senderInstanceNumber
                );
            } break;

            case Message::Type::AppendEntriesResults: {
                impl_->OnReceiveAppendEntriesResults(
                    message.appendEntriesResults,
                    senderInstanceNumber
                );
            } break;

            case Message::Type::InstallSnapshot: {
                impl_->OnReceiveInstallSnapshot(
                    message.installSnapshot,
                    std::move(message.snapshot),
                    senderInstanceNumber
                );
            } break;

            case Message::Type::InstallSnapshotResults: {
                impl_->OnReceiveInstallSnapshotResults(
                    message.installSnapshotResults,
                    senderInstanceNumber
                );
            } break;

            default: {
            } break;
        }
    }

    auto Server::GetElectionState() -> ElectionState {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        return impl_->shared->electionState;
    }

    void Server::AppendLogEntries(const std::vector< LogEntry >& entries) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        if (impl_->shared->electionState != ElectionState::Leader) {
            return;
        }
        impl_->AppendLogEntries(entries);
    }

    void Server::ChangeConfiguration(const ClusterConfiguration& newConfiguration) {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        if (impl_->shared->electionState != ElectionState::Leader) {
            return;
        }
        impl_->shared->diagnosticsSender.SendDiagnosticInformationFormatted(
            3,
            "ChangeConfiguration -- from %s to %s",
            FormatSet(impl_->shared->clusterConfiguration.instanceIds).c_str(),
            FormatSet(newConfiguration.instanceIds).c_str()
        );
        impl_->shared->configChangePending = true;
        impl_->shared->newServerCatchUpIndex = impl_->shared->commitIndex;
        impl_->ApplyConfiguration(
            impl_->shared->clusterConfiguration,
            newConfiguration
        );
    }

    void Server::ResetStatistics() {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        impl_->shared->broadcastTimeMeasurements.resize(NUM_BROADCAST_TIME_SAMPLES);
        impl_->shared->numBroadcastTimeMeasurements = 0;
        impl_->shared->broadcastTimeMeasurementsSum = 0;
        impl_->shared->minBroadcastTime = 0;
        impl_->shared->maxBroadcastTime = 0;
    }

    Json::Value Server::GetStatistics() {
        std::lock_guard< decltype(impl_->shared->mutex) > lock(impl_->shared->mutex);
        return Json::Object({
            {"minBroadcastTime", (double)impl_->shared->minBroadcastTime / 1000000.0},
            {
                "avgBroadcastTime", (
                    (impl_->shared->numBroadcastTimeMeasurements == 0)
                    ? 0.0
                    : (double)impl_->shared->broadcastTimeMeasurementsSum / (double)impl_->shared->numBroadcastTimeMeasurements / 1000000.0
                )
            },
            {"maxBroadcastTime", (double)impl_->shared->maxBroadcastTime / 1000000.0},
        });
    }

}
