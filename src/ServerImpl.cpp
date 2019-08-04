/**
 * @file ServerImpl.cpp
 *
 * This module contains the implementation of the Raft::Server::Impl structure.
 *
 * © 2018 by Richard Walters
 */

#include "ServerImpl.hpp"
#include "Utilities.hpp"

#include <algorithm>
#include <future>
#include <map>
#include <math.h>
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

namespace Raft {

    void Server::Impl::MeasureBroadcastTime(double sendTime) {
        const auto now = timeKeeper->GetCurrentTime();
        const auto measurement = (uintmax_t)(ceil((now - sendTime) * 1000000.0));
        if (shared->numBroadcastTimeMeasurements == 0) {
            shared->minBroadcastTime = measurement;
            shared->maxBroadcastTime = measurement;
            ++shared->numBroadcastTimeMeasurements;
        } else {
            shared->minBroadcastTime = std::min(shared->minBroadcastTime, measurement);
            shared->maxBroadcastTime = std::max(shared->maxBroadcastTime, measurement);
            if (shared->numBroadcastTimeMeasurements == shared->broadcastTimeMeasurements.size()) {
                shared->broadcastTimeMeasurementsSum -= shared->broadcastTimeMeasurements[shared->nextBroadcastTimeMeasurementIndex];
            } else {
                ++shared->numBroadcastTimeMeasurements;
            }
        }
        shared->broadcastTimeMeasurementsSum += measurement;
        shared->broadcastTimeMeasurements[shared->nextBroadcastTimeMeasurementIndex] = measurement;
        if (++shared->nextBroadcastTimeMeasurementIndex == shared->broadcastTimeMeasurements.size()) {
            shared->nextBroadcastTimeMeasurementIndex = 0;
        }
    }

    const std::set< int >& Server::Impl::GetInstanceIds() const {
        if (shared->jointConfiguration == nullptr) {
            return shared->clusterConfiguration.instanceIds;
        } else {
            return shared->jointConfiguration->instanceIds;
        }
    }

    bool Server::Impl::HaveNewServersCaughtUp() const {
        for (auto instanceId: shared->nextClusterConfiguration.instanceIds) {
            if (
                shared->clusterConfiguration.instanceIds.find(instanceId)
                != shared->clusterConfiguration.instanceIds.end()
            ) {
                continue;
            }
            const auto& instance = shared->instances[instanceId];
            if (instance.matchIndex < shared->newServerCatchUpIndex) {
                return false;
            }
        }
        return true;
    }

    void Server::Impl::InitializeInstanceInfo(InstanceInfo& instance) {
        instance.nextIndex = shared->lastIndex + 1;
        instance.matchIndex = 0;
    }

    void Server::Impl::ResetElectionTimer() {
        shared->timeOfLastLeaderMessage = timeKeeper->GetCurrentTime();
        shared->currentElectionTimeout = std::uniform_real_distribution<>(
            shared->serverConfiguration.minimumElectionTimeout,
            shared->serverConfiguration.maximumElectionTimeout
        )(shared->rng);
    }

    void Server::Impl::QueueMessageToBeSent(
        std::string message,
        int instanceNumber,
        double now,
        double timeout
    ) {
        auto& instance = shared->instances[instanceNumber];
        instance.timeLastRequestSent = now;
        instance.lastRequest = message;
        instance.timeout = timeout;
        const auto messageToBeSent = std::make_shared< SendMessageEvent >();
        messageToBeSent->serializedMessage = std::move(message);
        messageToBeSent->receiverInstanceNumber = instanceNumber;
        shared->eventQueue.push(messageToBeSent);
        workerAskedToStopOrWakeUp.notify_one();
    }

    void Server::Impl::QueueMessageToBeSent(
        const Message& message,
        int instanceNumber,
        double now
    ) {
        double timeout = shared->serverConfiguration.rpcTimeout;
        if (
            (message.type == Message::Type::RequestVote)
            || (message.type == Message::Type::AppendEntries)
            || (message.type == Message::Type::InstallSnapshot)
        ) {
            auto& instance = shared->instances[instanceNumber];
            instance.awaitingResponse = true;
            if (message.type == Message::Type::InstallSnapshot) {
                timeout = shared->serverConfiguration.installSnapshotTimeout;
            }
        }
        QueueMessageToBeSent(message.Serialize(), instanceNumber, now, timeout);
    }

    void Server::Impl::QueueLeadershipChangeAnnouncement(
        int leaderId,
        int term
    ) {
        shared->diagnosticsSender.SendDiagnosticInformationFormatted(
            2,
            "Server %d is now the leader in term %d",
            leaderId,
            term
        );
        const auto leadershipChangeEvent = std::make_shared< LeadershipChangeEvent >();
        leadershipChangeEvent->leaderId = leaderId;
        leadershipChangeEvent->term = term;
        shared->eventQueue.push(leadershipChangeEvent);
        workerAskedToStopOrWakeUp.notify_one();
    }

    void Server::Impl::QueueElectionStateChangeAnnouncement() {
        const auto electionStateEvent = std::make_shared< ElectionStateEvent >();
        electionStateEvent->term = shared->persistentStateCache.currentTerm;
        electionStateEvent->electionState = shared->electionState;
        electionStateEvent->didVote = shared->persistentStateCache.votedThisTerm;
        electionStateEvent->votedFor = shared->persistentStateCache.votedFor;
        shared->eventQueue.push(electionStateEvent);
        workerAskedToStopOrWakeUp.notify_one();
    }

    void Server::Impl::QueueConfigAppliedAnnouncement(
        const ClusterConfiguration& newConfiguration
    ) {
        const auto applyConfigurationEvent = std::make_shared< ApplyConfigurationEvent >();
        applyConfigurationEvent->newConfig = newConfiguration;
        shared->eventQueue.push(applyConfigurationEvent);
        workerAskedToStopOrWakeUp.notify_one();
    }

    void Server::Impl::QueueConfigCommittedAnnouncement(
        const ClusterConfiguration& newConfiguration,
        size_t logIndex
    ) {
        const auto commitConfigurationEvent = std::make_shared< CommitConfigurationEvent >();
        commitConfigurationEvent->newConfig = newConfiguration;
        commitConfigurationEvent->logIndex = logIndex;
        shared->eventQueue.push(commitConfigurationEvent);
        workerAskedToStopOrWakeUp.notify_one();
    }

    void Server::Impl::QueueCaughtUpAnnouncement() {
        const auto caughtUpEvent = std::make_shared< CaughtUpEvent >();
        shared->eventQueue.push(caughtUpEvent);
        workerAskedToStopOrWakeUp.notify_one();
    }

    void Server::Impl::QueueSnapshotAnnouncement(
        Json::Value&& snapshot,
        size_t lastIncludedIndex,
        int lastIncludedTerm
    ) {
        const auto snapshotInstalledEvent = std::make_shared< SnapshotInstalledEvent >();
        snapshotInstalledEvent->snapshot = std::move(snapshot);
        snapshotInstalledEvent->lastIncludedIndex = lastIncludedIndex;
        snapshotInstalledEvent->lastIncludedTerm = lastIncludedTerm;
        shared->eventQueue.push(snapshotInstalledEvent);
        workerAskedToStopOrWakeUp.notify_one();
    }

    void Server::Impl::ResetRetransmissionState() {
        for (auto instanceId: GetInstanceIds()) {
            shared->instances[instanceId].awaitingResponse = false;
        }
    }

    void Server::Impl::StepUpAsCandidate() {
        shared->electionState = IServer::ElectionState::Candidate;
        shared->persistentStateCache.votedThisTerm = true;
        shared->persistentStateCache.votedFor = shared->serverConfiguration.selfInstanceId;
        shared->persistentStateKeeper->Save(shared->persistentStateCache);
        if (
            shared->clusterConfiguration.instanceIds.find(shared->serverConfiguration.selfInstanceId)
            == shared->clusterConfiguration.instanceIds.end()
        ) {
            shared->votesForUsCurrentConfig = 0;
        } else {
            shared->votesForUsCurrentConfig = 1;
        }
        if (shared->jointConfiguration) {
            if (
                shared->nextClusterConfiguration.instanceIds.find(shared->serverConfiguration.selfInstanceId)
                == shared->nextClusterConfiguration.instanceIds.end()
            ) {
                shared->votesForUsNextConfig = 0;
            } else {
                shared->votesForUsNextConfig = 1;
            }
        }
        ResetRetransmissionState();
        shared->diagnosticsSender.SendDiagnosticInformationFormatted(
            3,
            "Timeout -- starting new election (term %d)",
            shared->persistentStateCache.currentTerm
        );
    }

    void Server::Impl::SendInitialVoteRequests(double now) {
        Message message;
        message.type = Message::Type::RequestVote;
        message.term = shared->persistentStateCache.currentTerm;
        message.requestVote.candidateId = shared->serverConfiguration.selfInstanceId;
        message.requestVote.lastLogIndex = shared->lastIndex;
        if (shared->lastIndex > 0) {
            message.requestVote.lastLogTerm = shared->logKeeper->GetTerm(shared->lastIndex);
        } else {
            message.requestVote.lastLogTerm = 0;
        }
        for (auto instanceNumber: GetInstanceIds()) {
            if (instanceNumber == shared->serverConfiguration.selfInstanceId) {
                continue;
            }
            auto& instance = shared->instances[instanceNumber];
            QueueMessageToBeSent(message, instanceNumber, now);
        }
        shared->timeOfLastLeaderMessage = timeKeeper->GetCurrentTime();
    }

    void Server::Impl::StartElection(double now) {
        UpdateCurrentTerm(shared->persistentStateCache.currentTerm + 1);
        StepUpAsCandidate();
        QueueElectionStateChangeAnnouncement();
        SendInitialVoteRequests(now);
        ResetElectionTimer();
    }

    void Server::Impl::AttemptLogReplication(int instanceId) {
        if (
            (
                shared->clusterConfiguration.instanceIds.find(instanceId)
                == shared->clusterConfiguration.instanceIds.end()
            )
            && (
                shared->nextClusterConfiguration.instanceIds.find(instanceId)
                == shared->nextClusterConfiguration.instanceIds.end()
            )
        ) {
            return;
        }
        auto& instance = shared->instances[instanceId];
        Message message;
        if (instance.nextIndex <= shared->logKeeper->GetBaseIndex()) {
            message.type = Message::Type::InstallSnapshot;
            message.term = shared->persistentStateCache.currentTerm;
            message.installSnapshot.lastIncludedIndex = shared->logKeeper->GetBaseIndex();
            message.installSnapshot.lastIncludedTerm = shared->logKeeper->GetTerm(
                message.installSnapshot.lastIncludedIndex
            );
            message.snapshot = shared->logKeeper->GetSnapshot();
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                3,
                "Installing snapshot on server %d (%zu entries, term %d)",
                instanceId,
                message.installSnapshot.lastIncludedIndex,
                message.installSnapshot.lastIncludedTerm
            );
        } else {
            message.type = Message::Type::AppendEntries;
            message.term = shared->persistentStateCache.currentTerm;
            message.appendEntries.leaderCommit = shared->commitIndex;
            message.appendEntries.prevLogIndex = instance.nextIndex - 1;
            message.appendEntries.prevLogTerm = shared->logKeeper->GetTerm(
                message.appendEntries.prevLogIndex
            );
            for (size_t i = instance.nextIndex; i <= shared->lastIndex; ++i) {
                message.log.push_back(shared->logKeeper->operator[](i));
            }
#ifdef EXTRA_DIAGNOSTICS
            if (shared->lastIndex < instance.nextIndex) {
                shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                    0,
                    "Replicating log to server %d (0 entries starting at %zu, term %d)",
                    instanceId,
                    instance.nextIndex,
                    shared->persistentStateCache.currentTerm
                );
            } else {
                shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                    2,
                    "Replicating log to server %d (%zu entries starting at %zu, term %d)",
                    instanceId,
                    (size_t)(shared->lastIndex - instance.nextIndex + 1),
                    instance.nextIndex,
                    shared->persistentStateCache.currentTerm
                );
                for (size_t i = instance.nextIndex; i <= shared->lastIndex; ++i) {
                    if (shared->logKeeper->operator[](i).command == nullptr) {
                        shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                            2,
                            "Entry #%zu of %zu: term=%d, no-op",
                            (size_t)(i - instance.nextIndex + 1),
                            (size_t)(shared->lastIndex - instance.nextIndex + 1),
                            shared->logKeeper->operator[](i).term
                        );
                    } else {
                        shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                            2,
                            "Entry #%zu of %zu: term=%d, command: '%s'",
                            (size_t)(i - instance.nextIndex + 1),
                            (size_t)(shared->lastIndex - instance.nextIndex + 1),
                            shared->logKeeper->operator[](i).term,
                            shared->logKeeper->operator[](i).command->GetType().c_str()
                        );
                    }
                }
            }
#endif /* EXTRA_DIAGNOSTICS */
        }
        QueueMessageToBeSent(
            message,
            instanceId,
            timeKeeper->GetCurrentTime()
        );
    }

    void Server::Impl::QueueHeartBeatsToBeSent(double now) {
        Message message;
        message.type = Message::Type::AppendEntries;
        message.term = shared->persistentStateCache.currentTerm;
        message.appendEntries.leaderCommit = shared->commitIndex;
        message.appendEntries.prevLogIndex = shared->lastIndex;
        message.appendEntries.prevLogTerm = shared->logKeeper->GetTerm(shared->lastIndex);
        shared->diagnosticsSender.SendDiagnosticInformationFormatted(
            0,
            "Sending heartbeat (term %d)",
            shared->persistentStateCache.currentTerm
        );
        for (auto instanceNumber: GetInstanceIds()) {
            auto& instance = shared->instances[instanceNumber];
            if (
                (instanceNumber == shared->serverConfiguration.selfInstanceId)
                || instance.awaitingResponse
            ) {
                continue;
            }
            QueueMessageToBeSent(message, instanceNumber, now);
        }
        shared->timeOfLastLeaderMessage = now;
        shared->sentHeartBeats = true;
    }

    void Server::Impl::QueueAppendEntriesToBeSent(
        double now,
        const std::vector< LogEntry >& entries
    ) {
        Message message;
        message.type = Message::Type::AppendEntries;
        message.term = shared->persistentStateCache.currentTerm;
        message.appendEntries.leaderCommit = shared->commitIndex;
        message.appendEntries.prevLogIndex = shared->lastIndex - entries.size();
        message.appendEntries.prevLogTerm = shared->logKeeper->GetTerm(message.appendEntries.prevLogIndex);
        message.log = entries;
#ifdef EXTRA_DIAGNOSTICS
        if (entries.empty()) {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                0,
                "Sending log entries (%zu entries starting at %zu, term %d)",
                entries.size(),
                message.appendEntries.prevLogIndex + 1,
                shared->persistentStateCache.currentTerm
            );
        } else {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                2,
                "Sending log entries (%zu entries starting at %zu, term %d)",
                entries.size(),
                message.appendEntries.prevLogIndex + 1,
                shared->persistentStateCache.currentTerm
            );
            for (size_t i = 0; i < entries.size(); ++i) {
                if (entries[i].command == nullptr) {
                    shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                        2,
                        "Entry #%zu of %zu: term=%d, no-op",
                        (size_t)(i + 1),
                        entries.size(),
                        entries[i].term
                    );
                } else {
                    shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                        2,
                        "Entry #%zu of %zu: term=%d, command: '%s'",
                        (size_t)(i + 1),
                        entries.size(),
                        entries[i].term,
                        entries[i].command->GetType().c_str()
                    );
                }
            }
        }
#endif /* EXTRA_DIAGNOSTICS */
        for (auto instanceNumber: GetInstanceIds()) {
            auto& instance = shared->instances[instanceNumber];
            if (
                (instanceNumber == shared->serverConfiguration.selfInstanceId)
                || instance.awaitingResponse
            ) {
                continue;
            }
            QueueMessageToBeSent(message, instanceNumber, now);
        }
        shared->timeOfLastLeaderMessage = timeKeeper->GetCurrentTime();
    }

    void Server::Impl::QueueRetransmissionsToBeSent(double now) {
        for (auto instanceId: GetInstanceIds()) {
            const auto& instance = shared->instances[instanceId];
            if (
                instance.awaitingResponse
                && (now - instance.timeLastRequestSent >= instance.timeout)
            ) {
#ifdef EXTRA_DIAGNOSTICS
                shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                    0,
                    "Retransmitting last message to server %d",
                    instanceId
                );
#endif
                QueueMessageToBeSent(
                    instance.lastRequest,
                    instanceId,
                    now
                );
            }
        }
    }

    void Server::Impl::ProcessEventQueue(
        std::unique_lock< decltype(shared->mutex) >& lock
    ) {
        decltype(shared->eventQueue) eventQueue;
        eventQueue.swap(shared->eventQueue);
        auto eventSubscribers = shared->eventSubscribers;
        lock.unlock();
        while (!eventQueue.empty()) {
            const auto event = eventQueue.front();
            for (auto eventSubscriber: eventSubscribers) {
                eventSubscriber.second(*event);
            }
            eventQueue.pop();
        }
        lock.lock();
    }

    void Server::Impl::UpdateCurrentTerm(int newTerm) {
        if (shared->persistentStateCache.currentTerm == newTerm) {
            return;
        }
        shared->thisTermLeaderAnnounced = false;
        shared->persistentStateCache.currentTerm = newTerm;
        shared->persistentStateCache.votedThisTerm = false;
        shared->persistentStateKeeper->Save(shared->persistentStateCache);
    }

    void Server::Impl::RevertToFollower() {
        ResetRetransmissionState();
        shared->electionState = IServer::ElectionState::Follower;
        shared->configChangePending = false;
        ResetElectionTimer();
    }

    void Server::Impl::AssumeLeadership() {
        ResetRetransmissionState();
        shared->electionState = IServer::ElectionState::Leader;
        shared->thisTermLeaderAnnounced = true;
        shared->leaderId = shared->serverConfiguration.selfInstanceId;
        shared->selfCatchUpIndex = shared->logKeeper->GetLastIndex();
        shared->sentHeartBeats = false;
        shared->diagnosticsSender.SendDiagnosticInformationString(
            3,
            "Received majority vote -- assuming leadership"
        );
        QueueElectionStateChangeAnnouncement();
        QueueLeadershipChangeAnnouncement(
            shared->serverConfiguration.selfInstanceId,
            shared->persistentStateCache.currentTerm
        );
        for (auto instanceId: GetInstanceIds()) {
            auto& instance = shared->instances[instanceId];
            InitializeInstanceInfo(instance);
        }
    }

    void Server::Impl::ApplyConfiguration(const ClusterConfiguration& clusterConfiguration) {
        shared->clusterConfiguration = clusterConfiguration;
        shared->jointConfiguration.reset();
        OnSetClusterConfiguration();
        QueueConfigAppliedAnnouncement(clusterConfiguration);
    }

    void Server::Impl::ApplyConfiguration(
        const ClusterConfiguration& clusterConfiguration,
        const ClusterConfiguration& nextClusterConfiguration
    ) {
        shared->clusterConfiguration = clusterConfiguration;
        shared->nextClusterConfiguration = nextClusterConfiguration;
        shared->jointConfiguration.reset(new ClusterConfiguration(clusterConfiguration));
        for (auto instanceId: nextClusterConfiguration.instanceIds) {
            (void)shared->jointConfiguration->instanceIds.insert(instanceId);
            if (
                (shared->electionState == ElectionState::Leader)
                && (shared->clusterConfiguration.instanceIds.find(instanceId) == shared->clusterConfiguration.instanceIds.end())
            ) {
                auto& instance = shared->instances[instanceId];
                InitializeInstanceInfo(instance);
            }
        }
        OnSetClusterConfiguration();
    }

    void Server::Impl::SetLastIndex(size_t newLastIndex) {
        if (newLastIndex < shared->lastIndex) {
            for (int i = (int)shared->lastIndex; i >= (int)newLastIndex; --i) {
                const auto& entry = shared->logKeeper->operator[](i);
                if (entry.command == nullptr) {
                    continue;
                }
                const auto commandType = entry.command->GetType();
                if (commandType == "SingleConfiguration") {
                    const auto command = std::static_pointer_cast< Raft::SingleConfigurationCommand >(entry.command);
                    shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                        3,
                        "Rolling back single configuration -- from %s to %s",
                        FormatSet(command->configuration.instanceIds).c_str(),
                        FormatSet(command->oldConfiguration.instanceIds).c_str()
                    );
                    ApplyConfiguration(command->oldConfiguration);
                } else if (commandType == "JointConfiguration") {
                    const auto command = std::static_pointer_cast< Raft::JointConfigurationCommand >(entry.command);
                    shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                        3,
                        "Rolling back joint configuration -- from %s to %s",
                        FormatSet(command->newConfiguration.instanceIds).c_str(),
                        FormatSet(command->oldConfiguration.instanceIds).c_str()
                    );
                    ApplyConfiguration(command->oldConfiguration);
                }
            }
        } else {
            for (size_t i = shared->lastIndex + 1; i <= newLastIndex; ++i) {
                const auto& entry = shared->logKeeper->operator[](i);
                if (entry.command == nullptr) {
                    continue;
                }
                const auto commandType = entry.command->GetType();
                if (commandType == "SingleConfiguration") {
                    const auto command = std::static_pointer_cast< Raft::SingleConfigurationCommand >(entry.command);
                    ApplyConfiguration(command->configuration);
                } else if (commandType == "JointConfiguration") {
                    const auto command = std::static_pointer_cast< Raft::JointConfigurationCommand >(entry.command);
                    ApplyConfiguration(
                        command->oldConfiguration,
                        command->newConfiguration
                    );
                }
            }
        }
        shared->lastIndex = newLastIndex;
    }

    bool Server::Impl::IsCommandApplied(
        std::function< bool(std::shared_ptr< ::Raft::Command > command) > visitor,
        size_t index
    ) {
        while (index <= shared->lastIndex) {
            const auto command = shared->logKeeper->operator[](index++).command;
            if (visitor(command)) {
                return true;
            }
        }
        return false;
    }

    bool Server::Impl::IsCommandApplied(
        const std::string& type,
        size_t index
    ) {
        return IsCommandApplied(
            [type](std::shared_ptr< ::Raft::Command > command){
                return (
                    (command != nullptr)
                    && (command->GetType() == type)
                );
            },
            index
        );
    }

    void Server::Impl::AdvanceCommitIndex(size_t newCommitIndex) {
        const auto lastCommitIndex = shared->commitIndex;
        const auto newCommitIndexWeHave = std::min(
            newCommitIndex,
            shared->logKeeper->GetLastIndex()
        );
        if (newCommitIndexWeHave != shared->commitIndex) {
#ifdef EXTRA_DIAGNOSTICS
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                2,
                "Advancing commit index %zu -> %zu (leader has %zu, we have %zu)",
                shared->commitIndex,
                newCommitIndexWeHave,
                newCommitIndex,
                shared->logKeeper->GetLastIndex()
            );
#endif /* EXTRA_DIAGNOSTICS */
            shared->commitIndex = newCommitIndexWeHave;
            shared->logKeeper->Commit(shared->commitIndex);
        }
        for (size_t i = lastCommitIndex + 1; i <= shared->commitIndex; ++i) {
            const auto& entry = shared->logKeeper->operator[](i);
            if (entry.command == nullptr) {
                continue;
            }
            const auto commandType = entry.command->GetType();
            if (commandType == "SingleConfiguration") {
                const auto command = std::static_pointer_cast< Raft::SingleConfigurationCommand >(entry.command);
                if (
                    (shared->electionState == ElectionState::Leader)
                    && !IsCommandApplied(
                        "JointConfiguration",
                        i + 1
                    )
                ) {
                    shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                        3,
                        "Single configuration committed: %s",
                        FormatSet(command->configuration.instanceIds).c_str()
                    );
                    if (
                        shared->clusterConfiguration.instanceIds.find(
                            shared->serverConfiguration.selfInstanceId
                        ) == shared->clusterConfiguration.instanceIds.end()
                    ) {
                        RevertToFollower();
                        QueueElectionStateChangeAnnouncement();
                    }
                    QueueConfigCommittedAnnouncement(
                        shared->clusterConfiguration,
                        i
                    );
                }
            } else if (commandType == "JointConfiguration") {
                if (
                    (shared->electionState == ElectionState::Leader)
                    && !IsCommandApplied(
                        "SingleConfiguration",
                        i + 1
                    )
                ) {
                    shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                        3,
                        "Joint configuration committed; applying new configuration -- from %s to %s",
                        FormatSet(shared->clusterConfiguration.instanceIds).c_str(),
                        FormatSet(shared->nextClusterConfiguration.instanceIds).c_str()
                    );
                    const auto command = std::make_shared< SingleConfigurationCommand >();
                    command->oldConfiguration = shared->clusterConfiguration;
                    command->configuration = shared->nextClusterConfiguration;
                    LogEntry entry;
                    entry.term = shared->persistentStateCache.currentTerm;
                    entry.command = std::move(command);
                    AppendLogEntries({std::move(entry)});
                }
            }
        }
        if (
            !shared->caughtUp
            && (shared->commitIndex >= shared->selfCatchUpIndex)
        ) {
            shared->caughtUp = true;
            QueueCaughtUpAnnouncement();
        }
    }

    void Server::Impl::AppendLogEntries(const std::vector< LogEntry >& entries) {
        shared->logKeeper->Append(entries);
        const auto now = timeKeeper->GetCurrentTime();
        SetLastIndex(shared->lastIndex + entries.size());
        QueueAppendEntriesToBeSent(now, entries);
    }

    void Server::Impl::StartConfigChangeIfNewServersHaveCaughtUp() {
        if (
            shared->configChangePending
            && HaveNewServersCaughtUp()
        ) {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                3,
                "Applying joint configuration -- from %s to %s",
                FormatSet(shared->clusterConfiguration.instanceIds).c_str(),
                FormatSet(shared->nextClusterConfiguration.instanceIds).c_str()
            );
            shared->configChangePending = false;
            const auto command = std::make_shared< JointConfigurationCommand >();
            command->oldConfiguration = shared->clusterConfiguration;
            command->newConfiguration = shared->nextClusterConfiguration;
            LogEntry entry;
            entry.term = shared->persistentStateCache.currentTerm;
            entry.command = std::move(command);
            AppendLogEntries({std::move(entry)});
        }
    }

    void Server::Impl::OnSetClusterConfiguration() {
        if (
            shared->clusterConfiguration.instanceIds.find(
                shared->serverConfiguration.selfInstanceId
            ) == shared->clusterConfiguration.instanceIds.end()
        ) {
            if (shared->jointConfiguration == nullptr) {
                shared->isVotingMember = false;
            } else {
                if (
                    shared->nextClusterConfiguration.instanceIds.find(
                        shared->serverConfiguration.selfInstanceId
                    ) == shared->nextClusterConfiguration.instanceIds.end()
                ) {
                    shared->isVotingMember = false;
                } else {
                    shared->isVotingMember = true;
                }
            }
        } else {
            shared->isVotingMember = true;
        }
        if (shared->jointConfiguration == nullptr) {
            auto instanceEntry = shared->instances.begin();
            while (instanceEntry != shared->instances.end()) {
                if (
                    shared->clusterConfiguration.instanceIds.find(instanceEntry->first)
                    == shared->clusterConfiguration.instanceIds.end()
                ) {
                    instanceEntry = shared->instances.erase(instanceEntry);
                } else {
                    ++instanceEntry;
                }
            }
        }
        if (shared->electionState == ElectionState::Leader) {
            StartConfigChangeIfNewServersHaveCaughtUp();
        }
    }

    void Server::Impl::OnReceiveRequestVote(
        const Message::RequestVoteDetails& messageDetails,
        int senderInstanceNumber,
        int senderTerm
    ) {
        const auto now = timeKeeper->GetCurrentTime();
        const auto termBeforeMessageProcessed = shared->persistentStateCache.currentTerm;
        if (
            shared->thisTermLeaderAnnounced
            && (
                now - shared->timeOfLastLeaderMessage
                < shared->serverConfiguration.minimumElectionTimeout
            )
        ) {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Ignoring vote for server %d for term %d (we were in term %d; vote requested before minimum election timeout)",
                senderInstanceNumber,
                senderTerm,
                termBeforeMessageProcessed
            );
            return;
        }
        if (senderTerm > shared->persistentStateCache.currentTerm) {
            UpdateCurrentTerm(senderTerm);
            RevertToFollower();
            QueueElectionStateChangeAnnouncement();
        }
        if (!shared->isVotingMember) {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Ignoring vote for server %d for term %d (we were in term %d, but non-voting member)",
                senderInstanceNumber,
                senderTerm,
                termBeforeMessageProcessed
            );
            return;
        }
        Message response;
        response.type = Message::Type::RequestVoteResults;
        response.term = shared->persistentStateCache.currentTerm;
        const auto lastIndex = shared->lastIndex;
        const auto lastTerm = shared->logKeeper->GetTerm(shared->lastIndex);
        if (shared->persistentStateCache.currentTerm > senderTerm) {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Rejecting vote for server %d (old term %d < %d)",
                senderInstanceNumber,
                senderTerm,
                shared->persistentStateCache.currentTerm
            );
            response.requestVoteResults.voteGranted = false;
        } else if (
            shared->persistentStateCache.votedThisTerm
            && (shared->persistentStateCache.votedFor != senderInstanceNumber)
        ) {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Rejecting vote for server %d (already voted for %u for term %d -- we were in term %d)",
                senderInstanceNumber,
                shared->persistentStateCache.votedFor,
                senderTerm,
                termBeforeMessageProcessed
            );
            response.requestVoteResults.voteGranted = false;
        } else if (
            (lastTerm > messageDetails.lastLogTerm)
            || (
                (lastTerm == messageDetails.lastLogTerm)
                && (lastIndex > messageDetails.lastLogIndex)
            )
        ) {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Rejecting vote for server %d (our log at %d:%d is more up to date than theirs at %d:%d)",
                senderInstanceNumber,
                lastIndex,
                lastTerm,
                messageDetails.lastLogIndex,
                messageDetails.lastLogTerm
            );
            response.requestVoteResults.voteGranted = false;
        } else {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Voting for server %d for term %d (we were in term %d)",
                senderInstanceNumber,
                senderTerm,
                termBeforeMessageProcessed
            );
            response.requestVoteResults.voteGranted = true;
            shared->persistentStateCache.votedThisTerm = true;
            shared->persistentStateCache.votedFor = senderInstanceNumber;
            shared->persistentStateKeeper->Save(shared->persistentStateCache);
        }
        QueueMessageToBeSent(response, senderInstanceNumber, now);
    }

    void Server::Impl::OnReceiveRequestVoteResults(
        const Message::RequestVoteResultsDetails& messageDetails,
        int senderInstanceNumber,
        int senderTerm
    ) {
        if (shared->electionState != ElectionState::Candidate) {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Stale vote from server %d in term %d ignored",
                senderInstanceNumber,
                senderTerm
            );
            return;
        }
        if (senderTerm > shared->persistentStateCache.currentTerm) {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Vote result from server %d in term %d when in term %d; reverted to follower",
                senderInstanceNumber,
                senderTerm,
                shared->persistentStateCache.currentTerm
            );
            UpdateCurrentTerm(senderTerm);
            RevertToFollower();
            QueueElectionStateChangeAnnouncement();
            return;
        }
        auto& instance = shared->instances[senderInstanceNumber];
        if (senderTerm < shared->persistentStateCache.currentTerm) {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Stale vote from server %d in term %d ignored",
                senderInstanceNumber,
                senderTerm
            );
            return;
        }
        if (messageDetails.voteGranted) {
            if (instance.awaitingResponse) {
                if (
                    shared->clusterConfiguration.instanceIds.find(senderInstanceNumber)
                    != shared->clusterConfiguration.instanceIds.end()
                ) {
                    ++shared->votesForUsCurrentConfig;
                }
                if (shared->jointConfiguration) {
                    if (
                        shared->nextClusterConfiguration.instanceIds.find(senderInstanceNumber)
                        != shared->nextClusterConfiguration.instanceIds.end()
                    ) {
                        ++shared->votesForUsNextConfig;
                    }
                    shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                        1,
                        "Server %d voted for us in term %d (%zu/%zu + %zu/%zu)",
                        senderInstanceNumber,
                        shared->persistentStateCache.currentTerm,
                        shared->votesForUsCurrentConfig,
                        shared->clusterConfiguration.instanceIds.size(),
                        shared->votesForUsNextConfig,
                        shared->nextClusterConfiguration.instanceIds.size()
                    );
                } else {
                    shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                        1,
                        "Server %d voted for us in term %d (%zu/%zu)",
                        senderInstanceNumber,
                        shared->persistentStateCache.currentTerm,
                        shared->votesForUsCurrentConfig,
                        shared->clusterConfiguration.instanceIds.size()
                    );
                }
                bool wonTheVote = (
                    shared->votesForUsCurrentConfig
                    > shared->clusterConfiguration.instanceIds.size() - shared->votesForUsCurrentConfig
                );
                if (shared->jointConfiguration) {
                    if (shared->votesForUsNextConfig
                        <= shared->nextClusterConfiguration.instanceIds.size() - shared->votesForUsNextConfig
                    ) {
                        wonTheVote = false;
                    }
                }
                if (
                    (shared->electionState == IServer::ElectionState::Candidate)
                    && wonTheVote
                ) {
                    AssumeLeadership();
                }
            } else {
                shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                    1,
                    "Repeat vote from server %d in term %d ignored",
                    senderInstanceNumber,
                    senderTerm
                );
            }
        } else {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Server %d refused to voted for us in term %d",
                senderInstanceNumber,
                shared->persistentStateCache.currentTerm
            );
        }
        instance.awaitingResponse = false;
    }

    void Server::Impl::OnReceiveAppendEntries(
        const Message::AppendEntriesDetails& messageDetails,
        std::vector< LogEntry >&& entries,
        int senderInstanceNumber,
        int senderTerm
    ) {
#ifdef EXTRA_DIAGNOSTICS
        if (entries.empty()) {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                0,
                "Received AppendEntries (heartbeat, last index %zu, term %d) from server %d in term %d (we are in term %d)",
                messageDetails.prevLogIndex,
                messageDetails.prevLogTerm,
                senderInstanceNumber,
                senderTerm,
                shared->persistentStateCache.currentTerm
            );
        } else {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                2,
                "Received AppendEntries (%zu entries building on %zu from term %d) from server %d in term %d (we are in term %d)",
                entries.size(),
                messageDetails.prevLogIndex,
                messageDetails.prevLogTerm,
                senderInstanceNumber,
                senderTerm,
                shared->persistentStateCache.currentTerm
            );
            for (size_t i = 0; i < entries.size(); ++i) {
                if (entries[i].command == nullptr) {
                    shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                        2,
                        "Entry #%zu of %zu: term=%d, no-op",
                        (size_t)(i + 1),
                        entries.size(),
                        entries[i].term
                    );
                } else {
                    shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                        2,
                        "Entry #%zu of %zu: term=%d, command: '%s'",
                        (size_t)(i + 1),
                        entries.size(),
                        entries[i].term,
                        entries[i].command->GetType().c_str()
                    );
                }
            }
        }
#endif /* EXTRA_DIAGNOSTICS */
        Message response;
        response.type = Message::Type::AppendEntriesResults;
        response.term = shared->persistentStateCache.currentTerm;
        if (shared->persistentStateCache.currentTerm > senderTerm) {
            response.appendEntriesResults.success = false;
            response.appendEntriesResults.matchIndex = 0;
        } else if (
            (shared->electionState == ElectionState::Leader)
            && (shared->persistentStateCache.currentTerm == senderTerm)
        ) {
            shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                SystemAbstractions::DiagnosticsSender::Levels::ERROR,
                "Received AppendEntries (%zu entries building on %zu from term %d) from server %d in SAME term %d",
                entries.size(),
                messageDetails.prevLogIndex,
                messageDetails.prevLogTerm,
                senderInstanceNumber,
                senderTerm
            );
            return;
        } else {
            bool electionStateChanged = (shared->electionState != ElectionState::Follower);
            if (
                (shared->electionState != ElectionState::Leader)
                || (shared->persistentStateCache.currentTerm < senderTerm)
            ) {
                if (shared->persistentStateCache.currentTerm < senderTerm) {
                    electionStateChanged = true;
                }
                UpdateCurrentTerm(senderTerm);
                if (!shared->thisTermLeaderAnnounced) {
                    shared->thisTermLeaderAnnounced = true;
                    shared->leaderId = senderInstanceNumber;
                    shared->processingMessageFromLeader = true;
                    QueueLeadershipChangeAnnouncement(
                        senderInstanceNumber,
                        shared->persistentStateCache.currentTerm
                    );
                }
            }
            RevertToFollower();
            if (electionStateChanged) {
                QueueElectionStateChangeAnnouncement();
            }
            if (shared->selfCatchUpIndex == 0) {
                shared->selfCatchUpIndex = messageDetails.prevLogIndex + entries.size();
            }
            if (messageDetails.leaderCommit > shared->commitIndex) {
                AdvanceCommitIndex(messageDetails.leaderCommit);
            }
            if (
                (messageDetails.prevLogIndex > shared->lastIndex)
                || (
                    shared->logKeeper->GetTerm(messageDetails.prevLogIndex)
                    != messageDetails.prevLogTerm
                )
            ) {
                if (messageDetails.prevLogIndex > shared->lastIndex) {
                    shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                        3,
                        "Mismatch in received AppendEntries (%zu > %zu)",
                        messageDetails.prevLogIndex,
                        shared->lastIndex
                    );
                } else {
                    shared->diagnosticsSender.SendDiagnosticInformationFormatted(
                        3,
                        "Mismatch in received AppendEntries (%zu <= %zu but %d != %d)",
                        messageDetails.prevLogIndex,
                        shared->lastIndex,
                        shared->logKeeper->GetTerm(messageDetails.prevLogIndex),
                        messageDetails.prevLogTerm
                    );
                }
                response.appendEntriesResults.success = false;
                response.appendEntriesResults.matchIndex = shared->logKeeper->GetBaseIndex();
            } else {
                response.appendEntriesResults.success = true;
                size_t nextIndex = messageDetails.prevLogIndex + 1;
                std::vector< LogEntry > entriesToAdd;
                bool conflictFound = false;
                for (size_t i = 0; i < entries.size(); ++i) {
                    auto& newEntry = entries[i];
                    const auto logIndex = messageDetails.prevLogIndex + i + 1;
                    if (
                        conflictFound
                        || (logIndex > shared->lastIndex)
                    ) {
                        entriesToAdd.push_back(std::move(newEntry));
                    } else {
                        if (shared->logKeeper->GetTerm(logIndex) != newEntry.term) {
                            conflictFound = true;
                            SetLastIndex(logIndex - 1);
                            shared->logKeeper->RollBack(logIndex - 1);
                            entriesToAdd.push_back(std::move(newEntry));
                        }
                    }
                }
                if (!entriesToAdd.empty()) {
                    shared->logKeeper->Append(entriesToAdd);
                }
                SetLastIndex(shared->logKeeper->GetLastIndex());
                response.appendEntriesResults.matchIndex = shared->lastIndex;
            }
        }
        const auto now = timeKeeper->GetCurrentTime();
        QueueMessageToBeSent(response, senderInstanceNumber, now);
    }

    void Server::Impl::OnReceiveAppendEntriesResults(
        const Message::AppendEntriesResultsDetails& messageDetails,
        int senderInstanceNumber,
        int senderTerm
    ) {
        auto& instance = shared->instances[senderInstanceNumber];
        if (instance.awaitingResponse) {
            MeasureBroadcastTime(instance.timeLastRequestSent);
        }
#ifdef EXTRA_DIAGNOSTICS
        shared->diagnosticsSender.SendDiagnosticInformationFormatted(
            1,
            "Received AppendEntriesResults(%s, term %d, match %zu, next %zu) from server %d (we are %s in term %d)",
            (messageDetails.success ? "success" : "failure"),
            senderTerm,
            messageDetails.matchIndex,
            instance.nextIndex,
            senderInstanceNumber,
            ElectionStateToString(shared->electionState).c_str(),
            shared->persistentStateCache.currentTerm
        );
#endif /* EXTRA_DIAGNOSTICS */
        if (senderTerm > shared->persistentStateCache.currentTerm) {
            UpdateCurrentTerm(senderTerm);
            RevertToFollower();
            QueueElectionStateChangeAnnouncement();
        }
        if (shared->electionState != ElectionState::Leader) {
            return;
        }
        instance.awaitingResponse = false;
        instance.matchIndex = messageDetails.matchIndex;
        instance.nextIndex = messageDetails.matchIndex + 1;
        if (instance.nextIndex <= shared->lastIndex) {
            AttemptLogReplication(senderInstanceNumber);
        }
        std::map< size_t, size_t > indexMatchCountsOldServers;
        std::map< size_t, size_t > indexMatchCountsNewServers;
        for (auto instanceId: GetInstanceIds()) {
            auto& instance = shared->instances[instanceId];
            if (instanceId == shared->serverConfiguration.selfInstanceId) {
                continue;
            }
            if (
                shared->clusterConfiguration.instanceIds.find(instanceId)
                != shared->clusterConfiguration.instanceIds.end()
            ) {
                ++indexMatchCountsOldServers[instance.matchIndex];
            }
            if (
                shared->nextClusterConfiguration.instanceIds.find(instanceId)
                != shared->nextClusterConfiguration.instanceIds.end()
            ) {
                ++indexMatchCountsNewServers[instance.matchIndex];
            }
        }
        size_t totalMatchCounts = 0;
        if (
            shared->clusterConfiguration.instanceIds.find(shared->serverConfiguration.selfInstanceId)
            != shared->clusterConfiguration.instanceIds.end()
        ) {
            ++totalMatchCounts;
        }
        for (
            auto indexMatchCountsOldServersEntry = indexMatchCountsOldServers.rbegin();
            indexMatchCountsOldServersEntry != indexMatchCountsOldServers.rend();
            ++indexMatchCountsOldServersEntry
        ) {
            totalMatchCounts += indexMatchCountsOldServersEntry->second;
            if (
                (indexMatchCountsOldServersEntry->first > shared->commitIndex)
                && (
                    totalMatchCounts
                    > shared->clusterConfiguration.instanceIds.size() - totalMatchCounts
                )
                && (
                    shared->logKeeper->GetTerm(indexMatchCountsOldServersEntry->first)
                    == shared->persistentStateCache.currentTerm
                )
            ) {
                if (shared->jointConfiguration) {
                    totalMatchCounts = 0;
                    if (
                        shared->nextClusterConfiguration.instanceIds.find(shared->serverConfiguration.selfInstanceId)
                        != shared->nextClusterConfiguration.instanceIds.end()
                    ) {
                        ++totalMatchCounts;
                    }
                    for (
                        auto indexMatchCountsNewServersEntry = indexMatchCountsNewServers.rbegin();
                        indexMatchCountsNewServersEntry != indexMatchCountsNewServers.rend();
                        ++indexMatchCountsNewServersEntry
                    ) {
                        totalMatchCounts += indexMatchCountsNewServersEntry->second;
                        if (
                            (indexMatchCountsNewServersEntry->first > shared->commitIndex)
                            && (
                                totalMatchCounts
                                > shared->nextClusterConfiguration.instanceIds.size() - totalMatchCounts
                            )
                            && (
                                shared->logKeeper->GetTerm(indexMatchCountsNewServersEntry->first)
                                == shared->persistentStateCache.currentTerm
                            )
                        ) {
                            AdvanceCommitIndex(
                                std::min(
                                    indexMatchCountsOldServersEntry->first,
                                    indexMatchCountsNewServersEntry->first
                                )
                            );
                            break;
                        }
                    }
                } else {
                    AdvanceCommitIndex(indexMatchCountsOldServersEntry->first);
                }
                break;
            }
        }
        StartConfigChangeIfNewServersHaveCaughtUp();
    }

    void Server::Impl::OnReceiveInstallSnapshot(
        const Message::InstallSnapshotDetails& messageDetails,
        Json::Value&& snapshot,
        int senderInstanceNumber,
        int senderTerm
    ) {
        shared->diagnosticsSender.SendDiagnosticInformationFormatted(
            3,
            "Received InstallSnapshot (%zu entries up to term %d) from server %d in term %d (we are in term %d)",
            messageDetails.lastIncludedIndex,
            messageDetails.lastIncludedTerm,
            senderInstanceNumber,
            senderTerm,
            shared->persistentStateCache.currentTerm
        );
        Message response;
        response.type = Message::Type::InstallSnapshotResults;
        response.term = shared->persistentStateCache.currentTerm;
        if (shared->persistentStateCache.currentTerm <= senderTerm) {
            bool electionStateChanged = (shared->electionState != ElectionState::Follower);
            if (shared->electionState != ElectionState::Leader) {
                if (shared->persistentStateCache.currentTerm < senderTerm) {
                    electionStateChanged = true;
                }
                UpdateCurrentTerm(senderTerm);
                if (!shared->thisTermLeaderAnnounced) {
                    shared->thisTermLeaderAnnounced = true;
                    shared->leaderId = senderInstanceNumber;
                    shared->processingMessageFromLeader = true;
                    QueueLeadershipChangeAnnouncement(
                        senderInstanceNumber,
                        shared->persistentStateCache.currentTerm
                    );
                }
            }
            RevertToFollower();
            if (electionStateChanged) {
                QueueElectionStateChangeAnnouncement();
            }
            if (shared->commitIndex < messageDetails.lastIncludedIndex) {
                shared->logKeeper->InstallSnapshot(
                    snapshot,
                    messageDetails.lastIncludedIndex,
                    messageDetails.lastIncludedTerm
                );
                shared->lastIndex = messageDetails.lastIncludedIndex;
                QueueSnapshotAnnouncement(
                    std::move(snapshot),
                    messageDetails.lastIncludedIndex,
                    messageDetails.lastIncludedTerm
                );
            }
            response.installSnapshotResults.matchIndex = shared->lastIndex;
            ResetElectionTimer();
        }
        const auto now = timeKeeper->GetCurrentTime();
        QueueMessageToBeSent(response, senderInstanceNumber, now);
    }

    void Server::Impl::OnReceiveInstallSnapshotResults(
        const Message::InstallSnapshotResultsDetails& messageDetails,
        int senderInstanceNumber,
        int senderTerm
    ) {
        auto& instance = shared->instances[senderInstanceNumber];
        if (instance.awaitingResponse) {
            MeasureBroadcastTime(instance.timeLastRequestSent);
        }
        shared->diagnosticsSender.SendDiagnosticInformationFormatted(
            1,
            "Received InstallSnapshotResults(term %d) from server %d (we are %s in term %d)",
            senderTerm,
            senderInstanceNumber,
            ElectionStateToString(shared->electionState).c_str(),
            shared->persistentStateCache.currentTerm
        );
        if (senderTerm > shared->persistentStateCache.currentTerm) {
            UpdateCurrentTerm(senderTerm);
            RevertToFollower();
            QueueElectionStateChangeAnnouncement();
        }
        if (shared->electionState != ElectionState::Leader) {
            return;
        }
        instance.awaitingResponse = false;
        instance.matchIndex = messageDetails.matchIndex;
        instance.nextIndex = messageDetails.matchIndex + 1;
        if (instance.nextIndex <= shared->lastIndex) {
            AttemptLogReplication(senderInstanceNumber);
        }
        // TODO: This really needs refactoring!
        // * Ugly code!
        // * Two instances of this code.
        std::map< size_t, size_t > indexMatchCountsOldServers;
        std::map< size_t, size_t > indexMatchCountsNewServers;
        for (auto instanceId: GetInstanceIds()) {
            auto& instance = shared->instances[instanceId];
            if (instanceId == shared->serverConfiguration.selfInstanceId) {
                continue;
            }
            if (
                shared->clusterConfiguration.instanceIds.find(instanceId)
                != shared->clusterConfiguration.instanceIds.end()
            ) {
                ++indexMatchCountsOldServers[instance.matchIndex];
            }
            if (
                shared->nextClusterConfiguration.instanceIds.find(instanceId)
                != shared->nextClusterConfiguration.instanceIds.end()
            ) {
                ++indexMatchCountsNewServers[instance.matchIndex];
            }
        }
        size_t totalMatchCounts = 0;
        if (
            shared->clusterConfiguration.instanceIds.find(shared->serverConfiguration.selfInstanceId)
            != shared->clusterConfiguration.instanceIds.end()
        ) {
            ++totalMatchCounts;
        }
        for (
            auto indexMatchCountsOldServersEntry = indexMatchCountsOldServers.rbegin();
            indexMatchCountsOldServersEntry != indexMatchCountsOldServers.rend();
            ++indexMatchCountsOldServersEntry
        ) {
            totalMatchCounts += indexMatchCountsOldServersEntry->second;
            if (
                (indexMatchCountsOldServersEntry->first > shared->commitIndex)
                && (
                    totalMatchCounts
                    > shared->clusterConfiguration.instanceIds.size() - totalMatchCounts
                )
                && (
                    shared->logKeeper->GetTerm(indexMatchCountsOldServersEntry->first)
                    == shared->persistentStateCache.currentTerm
                )
            ) {
                if (shared->jointConfiguration) {
                    totalMatchCounts = 0;
                    if (
                        shared->nextClusterConfiguration.instanceIds.find(shared->serverConfiguration.selfInstanceId)
                        != shared->nextClusterConfiguration.instanceIds.end()
                    ) {
                        ++totalMatchCounts;
                    }
                    for (
                        auto indexMatchCountsNewServersEntry = indexMatchCountsNewServers.rbegin();
                        indexMatchCountsNewServersEntry != indexMatchCountsNewServers.rend();
                        ++indexMatchCountsNewServersEntry
                    ) {
                        totalMatchCounts += indexMatchCountsNewServersEntry->second;
                        if (
                            (indexMatchCountsNewServersEntry->first > shared->commitIndex)
                            && (
                                totalMatchCounts
                                > shared->nextClusterConfiguration.instanceIds.size() - totalMatchCounts
                            )
                            && (
                                shared->logKeeper->GetTerm(indexMatchCountsNewServersEntry->first)
                                == shared->persistentStateCache.currentTerm
                            )
                        ) {
                            AdvanceCommitIndex(
                                std::min(
                                    indexMatchCountsOldServersEntry->first,
                                    indexMatchCountsNewServersEntry->first
                                )
                            );
                            break;
                        }
                    }
                } else {
                    AdvanceCommitIndex(indexMatchCountsOldServersEntry->first);
                }
                break;
            }
        }
        StartConfigChangeIfNewServersHaveCaughtUp();
    }

    void Server::Impl::WaitForWork(
        std::unique_lock< decltype(shared->mutex) >& lock,
        std::future< void >& workerAskedToStop
    ) {
        const auto rpcTimeoutMilliseconds = (int)(
            shared->serverConfiguration.rpcTimeout * 1000.0
        );
        (void)workerAskedToStopOrWakeUp.wait_for(
            lock,
            std::chrono::milliseconds(rpcTimeoutMilliseconds),
            [this, &workerAskedToStop]{
                return (
                    (
                        workerAskedToStop.wait_for(std::chrono::seconds(0))
                        == std::future_status::ready
                    )
                    || (shared->workerLoopCompletion != nullptr)
                    || !shared->eventQueue.empty()
                );
            }
        );
    }

    void Server::Impl::WorkerLoopBody(
        std::unique_lock< decltype(shared->mutex) >& lock
    ) {
        const auto now = timeKeeper->GetCurrentTime();
        const auto timeSinceLastLeaderMessage = (
            now - shared->timeOfLastLeaderMessage
        );
        if (shared->electionState == IServer::ElectionState::Leader) {
            if (
                !shared->sentHeartBeats
                || (
                    timeSinceLastLeaderMessage
                    >= shared->serverConfiguration.heartbeatInterval
                )
            ) {
                QueueHeartBeatsToBeSent(now);
            }
        } else {
            if (
                shared->isVotingMember
                && !shared->processingMessageFromLeader
                && (
                    timeSinceLastLeaderMessage
                    >= shared->currentElectionTimeout
                )
            ) {
                StartElection(now);
            }
        }
        QueueRetransmissionsToBeSent(now);
        ProcessEventQueue(lock);
    }

    void Server::Impl::Worker() {
        std::unique_lock< decltype(shared->mutex) > lock(shared->mutex);
        shared->diagnosticsSender.SendDiagnosticInformationString(
            0,
            "Worker thread started"
        );
        ResetElectionTimer();
        auto workerAskedToStop = stopWorker.get_future();
        while (
            workerAskedToStop.wait_for(std::chrono::seconds(0))
            != std::future_status::ready
        ) {
            WaitForWork(lock, workerAskedToStop);
            const auto signalWorkerLoopCompleted = (
                shared->workerLoopCompletion != nullptr
            );
            WorkerLoopBody(lock);
            if (signalWorkerLoopCompleted) {
                shared->workerLoopCompletion->set_value();
                shared->workerLoopCompletion = nullptr;
            }
        }
        if (shared->workerLoopCompletion != nullptr) {
            shared->workerLoopCompletion->set_value();
            shared->workerLoopCompletion = nullptr;
        }
        shared->diagnosticsSender.SendDiagnosticInformationString(
            0,
            "Worker thread stopping"
        );
    }

}
