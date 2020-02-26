/**
 * @file ServerImpl.cpp
 *
 * This module contains the implementation of the Raft::Server::Impl structure.
 *
 * © 2018-2020 by Richard Walters
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
#include <random>
#include <set>
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

    /**
     * This is the maximum number of time between leader messages samples to
     * measure before dropping old measurements.
     */
    constexpr size_t NUM_TIME_BETWEEN_LEADER_MESSAGES_SAMPLES = 1024;

}

namespace Raft {

    Server::Impl::Impl()
        : diagnosticsSender("Raft::Server")
    {
    }

    void Server::Impl::MeasureBroadcastTime(double sendTime) {
        const auto now = scheduler->GetClock()->GetCurrentTime();
        const auto measurement = (uintmax_t)(ceil((now - sendTime) * 1000000.0));
        if (numBroadcastTimeMeasurements == 0) {
            minBroadcastTime = measurement;
            maxBroadcastTime = measurement;
            ++numBroadcastTimeMeasurements;
        } else {
            minBroadcastTime = std::min(minBroadcastTime, measurement);
            maxBroadcastTime = std::max(maxBroadcastTime, measurement);
            if (numBroadcastTimeMeasurements == broadcastTimeMeasurements.size()) {
                broadcastTimeMeasurementsSum -= broadcastTimeMeasurements[nextBroadcastTimeMeasurementIndex];
            } else {
                ++numBroadcastTimeMeasurements;
            }
        }
        broadcastTimeMeasurementsSum += measurement;
        broadcastTimeMeasurements[nextBroadcastTimeMeasurementIndex] = measurement;
        if (++nextBroadcastTimeMeasurementIndex == broadcastTimeMeasurements.size()) {
            nextBroadcastTimeMeasurementIndex = 0;
        }
    }

    void Server::Impl::MeasureTimeBetweenLeaderMessages() {
        const auto now = scheduler->GetClock()->GetCurrentTime();
        const auto lastTime = timeLastMessageReceivedFromLeader;
        timeLastMessageReceivedFromLeader = now;
        if (lastTime == 0.0) {
            return;
        }
        const auto measurement = (uintmax_t)(ceil((now - lastTime) * 1000000.0));
        if (numTimeBetweenLeaderMessagesMeasurements == 0) {
            minTimeBetweenLeaderMessages = measurement;
            maxTimeBetweenLeaderMessages = measurement;
            ++numTimeBetweenLeaderMessagesMeasurements;
        } else {
            minTimeBetweenLeaderMessages = std::min(minTimeBetweenLeaderMessages, measurement);
            maxTimeBetweenLeaderMessages = std::max(maxTimeBetweenLeaderMessages, measurement);
            if (numTimeBetweenLeaderMessagesMeasurements == timeBetweenLeaderMessagesMeasurements.size()) {
                timeBetweenLeaderMessagesMeasurementsSum -= timeBetweenLeaderMessagesMeasurements[nextTimeBetweenLeaderMessagesMeasurementIndex];
            } else {
                ++numTimeBetweenLeaderMessagesMeasurements;
            }
        }
        timeBetweenLeaderMessagesMeasurementsSum += measurement;
        timeBetweenLeaderMessagesMeasurements[nextTimeBetweenLeaderMessagesMeasurementIndex] = measurement;
        if (++nextTimeBetweenLeaderMessagesMeasurementIndex == timeBetweenLeaderMessagesMeasurements.size()) {
            nextTimeBetweenLeaderMessagesMeasurementIndex = 0;
        }
    }

    const std::set< int >& Server::Impl::GetInstanceIds() const {
        if (jointConfiguration == nullptr) {
            return clusterConfiguration.instanceIds;
        } else {
            return jointConfiguration->instanceIds;
        }
    }

    bool Server::Impl::HaveNewServersCaughtUp() const {
        for (auto instanceId: nextClusterConfiguration.instanceIds) {
            if (
                clusterConfiguration.instanceIds.find(instanceId)
                != clusterConfiguration.instanceIds.end()
            ) {
                continue;
            }
            const auto instancesEntry = instances.find(instanceId);
            if (instancesEntry == instances.end()) {
                continue;
            }
            const auto& instance = instancesEntry->second;
            if (instance.matchIndex < newServerCatchUpIndex) {
                return false;
            }
        }
        return true;
    }

    void Server::Impl::InitializeInstanceInfo(InstanceInfo& instance) {
        instance.nextIndex = lastIndex + 1;
        instance.matchIndex = 0;
    }

    void Server::Impl::ResetElectionTimer() {
        if (electionTimeoutToken) {
            scheduler->Cancel(electionTimeoutToken);
        }
        const auto timeout = (
            scheduler->GetClock()->GetCurrentTime()
            + std::uniform_real_distribution<>(
                serverConfiguration.minimumElectionTimeout,
                serverConfiguration.maximumElectionTimeout
            )(rng)
        );
        std::weak_ptr< Impl > weakImpl(shared_from_this());
        const auto thisGeneration = generation;
        electionTimeoutToken = scheduler->Schedule(
            [weakImpl, thisGeneration]{
                auto impl = weakImpl.lock();
                if (impl == nullptr) {
                    return;
                }
                std::lock_guard< decltype(impl->mutex) > lock(impl->mutex);
                if (
                    !impl->mobilized
                    || (impl->generation != thisGeneration)
                ) {
                    return;
                }
                impl->electionTimeoutToken = 0;
                if (
                    (impl->electionState != ElectionState::Leader)
                    && impl->isVotingMember
                    && !impl->processingMessageFromLeader
                ) {
                    impl->StartElection(impl->scheduler->GetClock()->GetCurrentTime());
                }
            },
            timeout
        );
    }

    void Server::Impl::ResetHeartbeatTimer() {
        if (heartbeatTimeoutToken) {
            scheduler->Cancel(heartbeatTimeoutToken);
        }
        const auto timeout = (
            scheduler->GetClock()->GetCurrentTime()
            + serverConfiguration.heartbeatInterval
        );
        std::weak_ptr< Impl > weakImpl(shared_from_this());
        const auto thisGeneration = generation;
        heartbeatTimeoutToken = scheduler->Schedule(
            [weakImpl, thisGeneration]{
                auto impl = weakImpl.lock();
                if (impl == nullptr) {
                    return;
                }
                std::lock_guard< decltype(impl->mutex) > lock(impl->mutex);
                if (
                    !impl->mobilized
                    || (impl->generation != thisGeneration)
                ) {
                    return;
                }
                impl->heartbeatTimeoutToken = 0;
                if (impl->electionState == ElectionState::Leader) {
                    impl->QueueHeartBeatsToBeSent();
                }
            },
            timeout
        );
    }

    void Server::Impl::ResetStatistics() {
        broadcastTimeMeasurements.resize(NUM_BROADCAST_TIME_SAMPLES);
        numBroadcastTimeMeasurements = 0;
        broadcastTimeMeasurementsSum = 0;
        minBroadcastTime = 0;
        maxBroadcastTime = 0;
        timeBetweenLeaderMessagesMeasurements.resize(NUM_TIME_BETWEEN_LEADER_MESSAGES_SAMPLES);
        numTimeBetweenLeaderMessagesMeasurements = 0;
        timeBetweenLeaderMessagesMeasurementsSum = 0;
        minTimeBetweenLeaderMessages = 0;
        maxTimeBetweenLeaderMessages = 0;
    }

    void Server::Impl::QueueSerializedMessageToBeSent(
        const std::string& message,
        int instanceNumber,
        double now,
        double timeout
    ) {
        auto& instance = instances[instanceNumber];
        instance.timeLastRequestSent = now;
        instance.lastRequest = message;
        instance.timeout = timeout;
        const auto messageToBeSent = std::make_shared< SendMessageEvent >();
        messageToBeSent->serializedMessage = std::move(message);
        messageToBeSent->receiverInstanceNumber = instanceNumber;
        AddToEventQueue(std::move(messageToBeSent));
        std::weak_ptr< Impl > weakImpl(shared_from_this());
        if (instance.awaitingResponse) {
            const auto thisGeneration = generation;
            instance.retransmitSchedulerToken = scheduler->Schedule(
                [weakImpl, instanceNumber, thisGeneration]{
                    auto impl = weakImpl.lock();
                    if (impl == nullptr) {
                        return;
                    }
                    std::lock_guard< decltype(impl->mutex) > lock(impl->mutex);
                    if (
                        !impl->mobilized
                        || (impl->generation != thisGeneration)
                    ) {
                        return;
                    }
                    impl->QueueRetransmission(instanceNumber);
                },
                now + timeout
            );
        }
    }

    void Server::Impl::SerializeAndQueueMessageToBeSent(
        Message& message,
        int instanceNumber,
        double now
    ) {
        double timeout = serverConfiguration.rpcTimeout;
        if (
            (message.type == Message::Type::RequestVote)
            || (message.type == Message::Type::AppendEntries)
            || (message.type == Message::Type::InstallSnapshot)
        ) {
            auto& instance = instances[instanceNumber];
            instance.awaitingResponse = true;
            if (++instance.lastSerialNumber >= 0x7F) {
                instance.lastSerialNumber = 1;
            }
            message.seq = instance.lastSerialNumber;
            if (message.type == Message::Type::InstallSnapshot) {
                timeout = serverConfiguration.installSnapshotTimeout;
            }
        }
        QueueSerializedMessageToBeSent(message.Serialize(), instanceNumber, now, timeout);
    }

    void Server::Impl::QueueLeadershipChangeAnnouncement(
        int leaderId,
        int term
    ) {
        diagnosticsSender.SendDiagnosticInformationFormatted(
            2,
            "Server %d is now the leader in term %d",
            leaderId,
            term
        );
        const auto leadershipChangeEvent = std::make_shared< LeadershipChangeEvent >();
        leadershipChangeEvent->leaderId = leaderId;
        leadershipChangeEvent->term = term;
        AddToEventQueue(std::move(leadershipChangeEvent));
    }

    void Server::Impl::QueueElectionStateChangeAnnouncement() {
        const auto electionStateEvent = std::make_shared< ElectionStateEvent >();
        electionStateEvent->term = persistentStateCache.currentTerm;
        electionStateEvent->electionState = electionState;
        electionStateEvent->didVote = persistentStateCache.votedThisTerm;
        electionStateEvent->votedFor = persistentStateCache.votedFor;
        AddToEventQueue(std::move(electionStateEvent));
    }

    void Server::Impl::QueueConfigAppliedAnnouncement(
        const ClusterConfiguration& newConfiguration
    ) {
        const auto applyConfigurationEvent = std::make_shared< ApplyConfigurationEvent >();
        applyConfigurationEvent->newConfig = newConfiguration;
        AddToEventQueue(std::move(applyConfigurationEvent));
    }

    void Server::Impl::QueueConfigCommittedAnnouncement(
        const ClusterConfiguration& newConfiguration,
        size_t logIndex
    ) {
        const auto commitConfigurationEvent = std::make_shared< CommitConfigurationEvent >();
        commitConfigurationEvent->newConfig = newConfiguration;
        commitConfigurationEvent->logIndex = logIndex;
        AddToEventQueue(std::move(commitConfigurationEvent));
    }

    void Server::Impl::QueueCaughtUpAnnouncement() {
        const auto caughtUpEvent = std::make_shared< CaughtUpEvent >();
        AddToEventQueue(std::move(caughtUpEvent));
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
        AddToEventQueue(std::move(snapshotInstalledEvent));
    }

    void Server::Impl::CancelAllCallbacks() {
        for (auto instanceId: GetInstanceIds()) {
            auto& instance = instances[instanceId];
            CancelRetransmissionCallbacks(instance);
        }
        if (heartbeatTimeoutToken != 0) {
            scheduler->Cancel(heartbeatTimeoutToken);
            heartbeatTimeoutToken = 0;
        }
        if (electionTimeoutToken != 0) {
            scheduler->Cancel(electionTimeoutToken);
            electionTimeoutToken = 0;
        }
    }

    void Server::Impl::CancelRetransmissionCallbacks(InstanceInfo& instance) {
        instance.awaitingResponse = false;
        if (instance.retransmitSchedulerToken != 0) {
            scheduler->Cancel(instance.retransmitSchedulerToken);
            instance.retransmitSchedulerToken = 0;
        }
    }

    void Server::Impl::StepUpAsCandidate() {
        electionState = IServer::ElectionState::Candidate;
        persistentStateCache.votedThisTerm = true;
        persistentStateCache.votedFor = serverConfiguration.selfInstanceId;
        persistentStateKeeper->Save(persistentStateCache);
        if (
            clusterConfiguration.instanceIds.find(serverConfiguration.selfInstanceId)
            == clusterConfiguration.instanceIds.end()
        ) {
            votesForUsCurrentConfig = 0;
        } else {
            votesForUsCurrentConfig = 1;
        }
        if (jointConfiguration) {
            if (
                nextClusterConfiguration.instanceIds.find(serverConfiguration.selfInstanceId)
                == nextClusterConfiguration.instanceIds.end()
            ) {
                votesForUsNextConfig = 0;
            } else {
                votesForUsNextConfig = 1;
            }
        }
        CancelAllCallbacks();
        diagnosticsSender.SendDiagnosticInformationFormatted(
            3,
            "Timeout -- starting new election (term %d)",
            persistentStateCache.currentTerm
        );
    }

    void Server::Impl::SendInitialVoteRequests(double now) {
        Message message;
        message.type = Message::Type::RequestVote;
        message.term = persistentStateCache.currentTerm;
        message.requestVote.candidateId = serverConfiguration.selfInstanceId;
        message.requestVote.lastLogIndex = lastIndex;
        if (lastIndex > 0) {
            message.requestVote.lastLogTerm = logKeeper->GetTerm(lastIndex);
        } else {
            message.requestVote.lastLogTerm = 0;
        }
        for (auto instanceNumber: GetInstanceIds()) {
            if (instanceNumber == serverConfiguration.selfInstanceId) {
                continue;
            }
            auto& instance = instances[instanceNumber];
            SerializeAndQueueMessageToBeSent(message, instanceNumber, now);
        }
    }

    void Server::Impl::StartElection(double now) {
        UpdateCurrentTerm(persistentStateCache.currentTerm + 1);
        StepUpAsCandidate();
        QueueElectionStateChangeAnnouncement();
        SendInitialVoteRequests(now);
        ResetElectionTimer();
    }

    void Server::Impl::AttemptLogReplication(int instanceId) {
        if (
            (
                clusterConfiguration.instanceIds.find(instanceId)
                == clusterConfiguration.instanceIds.end()
            )
            && (
                nextClusterConfiguration.instanceIds.find(instanceId)
                == nextClusterConfiguration.instanceIds.end()
            )
        ) {
            return;
        }
        auto& instance = instances[instanceId];
        Message message;
        if (instance.nextIndex <= logKeeper->GetBaseIndex()) {
            message.type = Message::Type::InstallSnapshot;
            message.term = persistentStateCache.currentTerm;
            message.installSnapshot.lastIncludedIndex = logKeeper->GetBaseIndex();
            message.installSnapshot.lastIncludedTerm = logKeeper->GetTerm(
                message.installSnapshot.lastIncludedIndex
            );
            message.snapshot = logKeeper->GetSnapshot();
            diagnosticsSender.SendDiagnosticInformationFormatted(
                3,
                "Installing snapshot on server %d (%zu entries, term %d)",
                instanceId,
                message.installSnapshot.lastIncludedIndex,
                message.installSnapshot.lastIncludedTerm
            );
        } else {
            message.type = Message::Type::AppendEntries;
            message.term = persistentStateCache.currentTerm;
            message.appendEntries.leaderCommit = commitIndex;
            message.appendEntries.prevLogIndex = instance.nextIndex - 1;
            message.appendEntries.prevLogTerm = logKeeper->GetTerm(
                message.appendEntries.prevLogIndex
            );
            for (size_t i = instance.nextIndex; i <= lastIndex; ++i) {
                message.log.push_back(logKeeper->operator[](i));
            }
#ifdef EXTRA_DIAGNOSTICS
            if (lastIndex < instance.nextIndex) {
                diagnosticsSender.SendDiagnosticInformationFormatted(
                    0,
                    "Replicating log to server %d (0 entries starting at %zu, term %d)",
                    instanceId,
                    instance.nextIndex,
                    persistentStateCache.currentTerm
                );
            } else {
                diagnosticsSender.SendDiagnosticInformationFormatted(
                    2,
                    "Replicating log to server %d (%zu entries starting at %zu, term %d)",
                    instanceId,
                    (size_t)(lastIndex - instance.nextIndex + 1),
                    instance.nextIndex,
                    persistentStateCache.currentTerm
                );
                for (size_t i = instance.nextIndex; i <= lastIndex; ++i) {
                    if (logKeeper->operator[](i).command == nullptr) {
                        diagnosticsSender.SendDiagnosticInformationFormatted(
                            2,
                            "Entry #%zu of %zu: term=%d, no-op",
                            (size_t)(i - instance.nextIndex + 1),
                            (size_t)(lastIndex - instance.nextIndex + 1),
                            logKeeper->operator[](i).term
                        );
                    } else {
                        diagnosticsSender.SendDiagnosticInformationFormatted(
                            2,
                            "Entry #%zu of %zu: term=%d, command: '%s'",
                            (size_t)(i - instance.nextIndex + 1),
                            (size_t)(lastIndex - instance.nextIndex + 1),
                            logKeeper->operator[](i).term,
                            logKeeper->operator[](i).command->GetType().c_str()
                        );
                    }
                }
            }
#endif /* EXTRA_DIAGNOSTICS */
        }
        const auto now = scheduler->GetClock()->GetCurrentTime();
        timeOfLastLeaderMessage = now;
        SerializeAndQueueMessageToBeSent(
            message,
            instanceId,
            now
        );
    }

    void Server::Impl::QueueHeartBeatsToBeSent() {
        Message message;
        message.type = Message::Type::AppendEntries;
        message.term = persistentStateCache.currentTerm;
        message.appendEntries.leaderCommit = commitIndex;
        message.appendEntries.prevLogIndex = lastIndex;
        message.appendEntries.prevLogTerm = logKeeper->GetTerm(lastIndex);
        diagnosticsSender.SendDiagnosticInformationFormatted(
            0,
            "Sending heartbeat (term %d)",
            persistentStateCache.currentTerm
        );
        const auto now = scheduler->GetClock()->GetCurrentTime();
        timeOfLastLeaderMessage = now;
        for (auto instanceNumber: GetInstanceIds()) {
            auto& instance = instances[instanceNumber];
            if (
                (instanceNumber == serverConfiguration.selfInstanceId)
                || instance.awaitingResponse
            ) {
                continue;
            }
            SerializeAndQueueMessageToBeSent(message, instanceNumber, now);
        }
        ResetHeartbeatTimer();
    }

    void Server::Impl::QueueAppendEntriesToBeSent(
        double now,
        const std::vector< LogEntry >& entries
    ) {
        Message message;
        message.type = Message::Type::AppendEntries;
        message.term = persistentStateCache.currentTerm;
        message.appendEntries.leaderCommit = commitIndex;
        message.appendEntries.prevLogIndex = lastIndex - entries.size();
        message.appendEntries.prevLogTerm = logKeeper->GetTerm(message.appendEntries.prevLogIndex);
        message.log = entries;
#ifdef EXTRA_DIAGNOSTICS
        if (entries.empty()) {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                0,
                "Sending log entries (%zu entries starting at %zu, term %d)",
                entries.size(),
                message.appendEntries.prevLogIndex + 1,
                persistentStateCache.currentTerm
            );
        } else {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                2,
                "Sending log entries (%zu entries starting at %zu, term %d)",
                entries.size(),
                message.appendEntries.prevLogIndex + 1,
                persistentStateCache.currentTerm
            );
            for (size_t i = 0; i < entries.size(); ++i) {
                if (entries[i].command == nullptr) {
                    diagnosticsSender.SendDiagnosticInformationFormatted(
                        2,
                        "Entry #%zu of %zu: term=%d, no-op",
                        (size_t)(i + 1),
                        entries.size(),
                        entries[i].term
                    );
                } else {
                    diagnosticsSender.SendDiagnosticInformationFormatted(
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
        timeOfLastLeaderMessage = now;
        for (auto instanceNumber: GetInstanceIds()) {
            auto& instance = instances[instanceNumber];
            if (
                (instanceNumber == serverConfiguration.selfInstanceId)
                || instance.awaitingResponse
            ) {
                continue;
            }
            SerializeAndQueueMessageToBeSent(message, instanceNumber, now);
        }
        ResetHeartbeatTimer();
    }

    void Server::Impl::QueueRetransmission(int instanceId) {
        const auto instancesEntry = instances.find(instanceId);
        if (instancesEntry == instances.end()) {
            return;
        }
        const auto& instance = instancesEntry->second;
        if (instance.awaitingResponse) {
#ifdef EXTRA_DIAGNOSTICS
            diagnosticsSender.SendDiagnosticInformationFormatted(
                0,
                "Retransmitting last message to server %d",
                instanceId
            );
#endif
            QueueSerializedMessageToBeSent(
                instance.lastRequest,
                instanceId,
                scheduler->GetClock()->GetCurrentTime(),
                instance.timeout
            );
        }
    }

    void Server::Impl::AddToEventQueue(
        std::shared_ptr< Raft::IServer::Event >&& event
    ) {
        std::lock_guard< decltype(eventQueueMutex) > lock(eventQueueMutex);
        eventQueue.Add(std::move(event));
        eventQueueWorkerWakeCondition.notify_one();
    }

    void Server::Impl::ProcessEventQueue(
        std::unique_lock< decltype(eventQueueMutex) >& lock
    ) {
        auto eventSubscribersSample = eventSubscribers;
        lock.unlock();
        while (!eventQueue.IsEmpty()) {
            const auto event = eventQueue.Remove();
            for (auto eventSubscriber: eventSubscribersSample) {
                eventSubscriber.second(*event);
            }
        }
        lock.lock();
    }

    void Server::Impl::UpdateCurrentTerm(int newTerm) {
        if (persistentStateCache.currentTerm == newTerm) {
            return;
        }
        diagnosticsSender.SendDiagnosticInformationFormatted(
            1,
            "Updating term (was %d, now %d)",
            persistentStateCache.currentTerm,
            newTerm
        );
        thisTermLeaderAnnounced = false;
        persistentStateCache.currentTerm = newTerm;
        persistentStateCache.votedThisTerm = false;
        persistentStateKeeper->Save(persistentStateCache);
    }

    void Server::Impl::RevertToFollower() {
        if (electionState != IServer::ElectionState::Follower) {
            ResetStatistics();
            electionState = IServer::ElectionState::Follower;
            configChangePending = false;
            CancelAllCallbacks();
#ifdef EXTRA_DIAGNOSTICS
            diagnosticsSender.SendDiagnosticInformationString(
                2,
                "Reverted to follower"
            );
#endif /* EXTRA_DIAGNOSTICS */
        }
        ResetElectionTimer();
    }

    void Server::Impl::AssumeLeadership() {
        CancelAllCallbacks();
        electionState = IServer::ElectionState::Leader;
        thisTermLeaderAnnounced = true;
        leaderId = serverConfiguration.selfInstanceId;
        selfCatchUpIndex = logKeeper->GetLastIndex();
        diagnosticsSender.SendDiagnosticInformationString(
            3,
            "Received majority vote -- assuming leadership"
        );
        QueueElectionStateChangeAnnouncement();
        QueueLeadershipChangeAnnouncement(
            serverConfiguration.selfInstanceId,
            persistentStateCache.currentTerm
        );
        for (auto instanceId: GetInstanceIds()) {
            auto& instance = instances[instanceId];
            InitializeInstanceInfo(instance);
        }
        QueueHeartBeatsToBeSent();
    }

    void Server::Impl::ApplyConfiguration(const ClusterConfiguration& newClusterConfiguration) {
        clusterConfiguration = newClusterConfiguration;
        jointConfiguration.reset();
        OnSetClusterConfiguration();
        QueueConfigAppliedAnnouncement(clusterConfiguration);
    }

    void Server::Impl::ApplyConfiguration(
        const ClusterConfiguration& newClusterConfiguration,
        const ClusterConfiguration& newNextClusterConfiguration
    ) {
        clusterConfiguration = newClusterConfiguration;
        nextClusterConfiguration = newNextClusterConfiguration;
        jointConfiguration.reset(new ClusterConfiguration(clusterConfiguration));
        for (auto instanceId: nextClusterConfiguration.instanceIds) {
            (void)jointConfiguration->instanceIds.insert(instanceId);
            if (
                (electionState == ElectionState::Leader)
                && (clusterConfiguration.instanceIds.find(instanceId) == clusterConfiguration.instanceIds.end())
            ) {
                auto& instance = instances[instanceId];
                InitializeInstanceInfo(instance);
            }
        }
        OnSetClusterConfiguration();
    }

    void Server::Impl::SetLastIndex(size_t newLastIndex) {
        if (newLastIndex < lastIndex) {
            for (int i = (int)lastIndex; i >= (int)newLastIndex; --i) {
                const auto& entry = logKeeper->operator[](i);
                if (entry.command == nullptr) {
                    continue;
                }
                const auto commandType = entry.command->GetType();
                if (commandType == "SingleConfiguration") {
                    const auto command = std::static_pointer_cast< Raft::SingleConfigurationCommand >(entry.command);
                    diagnosticsSender.SendDiagnosticInformationFormatted(
                        3,
                        "Rolling back single configuration -- from %s to %s",
                        FormatSet(command->configuration.instanceIds).c_str(),
                        FormatSet(command->oldConfiguration.instanceIds).c_str()
                    );
                    ApplyConfiguration(command->oldConfiguration);
                } else if (commandType == "JointConfiguration") {
                    const auto command = std::static_pointer_cast< Raft::JointConfigurationCommand >(entry.command);
                    diagnosticsSender.SendDiagnosticInformationFormatted(
                        3,
                        "Rolling back joint configuration -- from %s to %s",
                        FormatSet(command->newConfiguration.instanceIds).c_str(),
                        FormatSet(command->oldConfiguration.instanceIds).c_str()
                    );
                    ApplyConfiguration(command->oldConfiguration);
                }
            }
        } else {
            for (size_t i = lastIndex + 1; i <= newLastIndex; ++i) {
                const auto& entry = logKeeper->operator[](i);
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
        lastIndex = newLastIndex;
    }

    bool Server::Impl::IsCommandApplied(
        std::function< bool(std::shared_ptr< ::Raft::Command > command) > visitor,
        size_t index
    ) {
        while (index <= lastIndex) {
            const auto command = logKeeper->operator[](index++).command;
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
        const auto lastCommitIndex = commitIndex;
        const auto newCommitIndexWeHave = std::min(
            newCommitIndex,
            logKeeper->GetLastIndex()
        );
        if (newCommitIndexWeHave != commitIndex) {
#ifdef EXTRA_DIAGNOSTICS
            diagnosticsSender.SendDiagnosticInformationFormatted(
                2,
                "Advancing commit index %zu -> %zu (leader has %zu, we have %zu)",
                commitIndex,
                newCommitIndexWeHave,
                newCommitIndex,
                logKeeper->GetLastIndex()
            );
#endif /* EXTRA_DIAGNOSTICS */
            commitIndex = newCommitIndexWeHave;
            logKeeper->Commit(commitIndex);
        }
        for (size_t i = lastCommitIndex + 1; i <= commitIndex; ++i) {
            const auto& entry = logKeeper->operator[](i);
            if (entry.command == nullptr) {
                continue;
            }
            const auto commandType = entry.command->GetType();
            if (commandType == "SingleConfiguration") {
                const auto command = std::static_pointer_cast< Raft::SingleConfigurationCommand >(entry.command);
                if (
                    (electionState == ElectionState::Leader)
                    && !IsCommandApplied(
                        "JointConfiguration",
                        i + 1
                    )
                ) {
                    diagnosticsSender.SendDiagnosticInformationFormatted(
                        3,
                        "Single configuration committed: %s",
                        FormatSet(command->configuration.instanceIds).c_str()
                    );
                    if (
                        clusterConfiguration.instanceIds.find(
                            serverConfiguration.selfInstanceId
                        ) == clusterConfiguration.instanceIds.end()
                    ) {
                        RevertToFollower();
                        QueueElectionStateChangeAnnouncement();
                    }
                    QueueConfigCommittedAnnouncement(
                        clusterConfiguration,
                        i
                    );
                }
            } else if (commandType == "JointConfiguration") {
                if (
                    (electionState == ElectionState::Leader)
                    && !IsCommandApplied(
                        "SingleConfiguration",
                        i + 1
                    )
                ) {
                    diagnosticsSender.SendDiagnosticInformationFormatted(
                        3,
                        "Joint configuration committed; applying new configuration -- from %s to %s",
                        FormatSet(clusterConfiguration.instanceIds).c_str(),
                        FormatSet(nextClusterConfiguration.instanceIds).c_str()
                    );
                    const auto command = std::make_shared< SingleConfigurationCommand >();
                    command->oldConfiguration = clusterConfiguration;
                    command->configuration = nextClusterConfiguration;
                    LogEntry entry;
                    entry.term = persistentStateCache.currentTerm;
                    entry.command = std::move(command);
                    AppendLogEntries({std::move(entry)});
                }
            }
        }
        if (
            !caughtUp
            && (commitIndex >= selfCatchUpIndex)
        ) {
            caughtUp = true;
            QueueCaughtUpAnnouncement();
        }
    }

    void Server::Impl::AppendLogEntries(const std::vector< LogEntry >& entries) {
        logKeeper->Append(entries);
        const auto now = scheduler->GetClock()->GetCurrentTime();
        SetLastIndex(lastIndex + entries.size());
        QueueAppendEntriesToBeSent(now, entries);
    }

    void Server::Impl::StartConfigChangeIfNewServersHaveCaughtUp() {
        if (
            configChangePending
            && HaveNewServersCaughtUp()
        ) {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                3,
                "Applying joint configuration -- from %s to %s",
                FormatSet(clusterConfiguration.instanceIds).c_str(),
                FormatSet(nextClusterConfiguration.instanceIds).c_str()
            );
            configChangePending = false;
            const auto command = std::make_shared< JointConfigurationCommand >();
            command->oldConfiguration = clusterConfiguration;
            command->newConfiguration = nextClusterConfiguration;
            LogEntry entry;
            entry.term = persistentStateCache.currentTerm;
            entry.command = std::move(command);
            AppendLogEntries({std::move(entry)});
        }
    }

    void Server::Impl::OnSetClusterConfiguration() {
        if (
            clusterConfiguration.instanceIds.find(
                serverConfiguration.selfInstanceId
            ) == clusterConfiguration.instanceIds.end()
        ) {
            if (jointConfiguration == nullptr) {
                isVotingMember = false;
            } else {
                if (
                    nextClusterConfiguration.instanceIds.find(
                        serverConfiguration.selfInstanceId
                    ) == nextClusterConfiguration.instanceIds.end()
                ) {
                    isVotingMember = false;
                } else {
                    isVotingMember = true;
                }
            }
        } else {
            isVotingMember = true;
        }
        if (jointConfiguration == nullptr) {
            auto instanceEntry = instances.begin();
            while (instanceEntry != instances.end()) {
                if (
                    clusterConfiguration.instanceIds.find(instanceEntry->first)
                    == clusterConfiguration.instanceIds.end()
                ) {
                    instanceEntry = instances.erase(instanceEntry);
                } else {
                    ++instanceEntry;
                }
            }
        }
        if (electionState == ElectionState::Leader) {
            StartConfigChangeIfNewServersHaveCaughtUp();
        }
        diagnosticsSender.SendDiagnosticInformationString(
            3,
            "Cluster configuration changed"
        );
    }

    void Server::Impl::OnReceiveRequestVote(
        Message&& message,
        int senderInstanceNumber
    ) {
        const auto now = scheduler->GetClock()->GetCurrentTime();
        const auto termBeforeMessageProcessed = persistentStateCache.currentTerm;
        if (
            thisTermLeaderAnnounced
            && (
                now - timeOfLastLeaderMessage
                < serverConfiguration.minimumElectionTimeout
            )
        ) {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Ignoring vote for server %d for term %d (we were in term %d; vote requested before minimum election timeout)",
                senderInstanceNumber,
                message.term,
                termBeforeMessageProcessed
            );
            return;
        }
        if (message.term > persistentStateCache.currentTerm) {
            UpdateCurrentTerm(message.term);
            RevertToFollower();
        }
        if (!isVotingMember) {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Ignoring vote for server %d for term %d (we were in term %d, but non-voting member)",
                senderInstanceNumber,
                message.term,
                termBeforeMessageProcessed
            );
            QueueElectionStateChangeAnnouncement();
            return;
        }
        Message response;
        response.type = Message::Type::RequestVoteResults;
        response.term = persistentStateCache.currentTerm;
        response.seq = message.seq;
        const auto lastTerm = logKeeper->GetTerm(lastIndex);
        if (persistentStateCache.currentTerm > message.term) {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Rejecting vote for server %d (old term %d < %d)",
                senderInstanceNumber,
                message.term,
                persistentStateCache.currentTerm
            );
            response.requestVoteResults.voteGranted = false;
        } else if (
            persistentStateCache.votedThisTerm
            && (persistentStateCache.votedFor != senderInstanceNumber)
        ) {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Rejecting vote for server %d (already voted for %u for term %d -- we were in term %d)",
                senderInstanceNumber,
                persistentStateCache.votedFor,
                message.term,
                termBeforeMessageProcessed
            );
            response.requestVoteResults.voteGranted = false;
        } else if (
            (lastTerm > message.requestVote.lastLogTerm)
            || (
                (lastTerm == message.requestVote.lastLogTerm)
                && (lastIndex > message.requestVote.lastLogIndex)
            )
        ) {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Rejecting vote for server %d (our log at %d:%d is more up to date than theirs at %d:%d)",
                senderInstanceNumber,
                lastIndex,
                lastTerm,
                message.requestVote.lastLogIndex,
                message.requestVote.lastLogTerm
            );
            response.requestVoteResults.voteGranted = false;
        } else {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Voting for server %d for term %d (we were in term %d)",
                senderInstanceNumber,
                message.term,
                termBeforeMessageProcessed
            );
            response.requestVoteResults.voteGranted = true;
            persistentStateCache.votedThisTerm = true;
            persistentStateCache.votedFor = senderInstanceNumber;
            persistentStateKeeper->Save(persistentStateCache);
        }
        QueueElectionStateChangeAnnouncement();
        SerializeAndQueueMessageToBeSent(response, senderInstanceNumber, now);
    }

    void Server::Impl::OnReceiveRequestVoteResults(
        Message&& message,
        int senderInstanceNumber
    ) {
        if (electionState != ElectionState::Candidate) {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Stale vote from server %d in term %d ignored",
                senderInstanceNumber,
                message.term
            );
            return;
        }
        if (message.term > persistentStateCache.currentTerm) {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Vote result from server %d in term %d when in term %d; reverted to follower",
                senderInstanceNumber,
                message.term,
                persistentStateCache.currentTerm
            );
            UpdateCurrentTerm(message.term);
            RevertToFollower();
            QueueElectionStateChangeAnnouncement();
            return;
        }
        auto& instance = instances[senderInstanceNumber];
        if (message.term < persistentStateCache.currentTerm) {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Stale vote from server %d in term %d ignored",
                senderInstanceNumber,
                message.term
            );
            return;
        }
        if (
            !instance.awaitingResponse
            || (
                (message.seq != 0)
                && (instance.lastSerialNumber != message.seq)
            )
        ) {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Unexpected vote from server %d in term %d ignored",
                senderInstanceNumber,
                message.term
            );
            return;
        }
        CancelRetransmissionCallbacks(instance);
        if (message.requestVoteResults.voteGranted) {
            if (
                clusterConfiguration.instanceIds.find(senderInstanceNumber)
                != clusterConfiguration.instanceIds.end()
            ) {
                ++votesForUsCurrentConfig;
            }
            if (jointConfiguration) {
                if (
                    nextClusterConfiguration.instanceIds.find(senderInstanceNumber)
                    != nextClusterConfiguration.instanceIds.end()
                ) {
                    ++votesForUsNextConfig;
                }
                diagnosticsSender.SendDiagnosticInformationFormatted(
                    1,
                    "Server %d voted for us in term %d (%zu/%zu + %zu/%zu)",
                    senderInstanceNumber,
                    persistentStateCache.currentTerm,
                    votesForUsCurrentConfig,
                    clusterConfiguration.instanceIds.size(),
                    votesForUsNextConfig,
                    nextClusterConfiguration.instanceIds.size()
                );
            } else {
                diagnosticsSender.SendDiagnosticInformationFormatted(
                    1,
                    "Server %d voted for us in term %d (%zu/%zu)",
                    senderInstanceNumber,
                    persistentStateCache.currentTerm,
                    votesForUsCurrentConfig,
                    clusterConfiguration.instanceIds.size()
                );
            }
            bool wonTheVote = (
                votesForUsCurrentConfig
                > clusterConfiguration.instanceIds.size() - votesForUsCurrentConfig
            );
            if (jointConfiguration) {
                if (votesForUsNextConfig
                    <= nextClusterConfiguration.instanceIds.size() - votesForUsNextConfig
                ) {
                    wonTheVote = false;
                }
            }
            if (
                (electionState == IServer::ElectionState::Candidate)
                && wonTheVote
            ) {
                AssumeLeadership();
            }
        } else {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                1,
                "Server %d refused to voted for us in term %d",
                senderInstanceNumber,
                persistentStateCache.currentTerm
            );
        }
    }

    void Server::Impl::OnReceiveAppendEntries(
        Message&& message,
        int senderInstanceNumber
    ) {
#ifdef EXTRA_DIAGNOSTICS
        if (message.log.empty()) {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                0,
                "Received AppendEntries (heartbeat, last index %zu, term %d) from server %d in term %d (we are in term %d)",
                message.appendEntries.prevLogIndex,
                message.appendEntries.prevLogTerm,
                senderInstanceNumber,
                message.term,
                persistentStateCache.currentTerm
            );
        } else {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                2,
                "Received AppendEntries (%zu entries building on %zu from term %d) from server %d in term %d (we are in term %d)",
                message.log.size(),
                message.appendEntries.prevLogIndex,
                message.appendEntries.prevLogTerm,
                senderInstanceNumber,
                message.term,
                persistentStateCache.currentTerm
            );
            for (size_t i = 0; i < message.log.size(); ++i) {
                if (message.log[i].command == nullptr) {
                    diagnosticsSender.SendDiagnosticInformationFormatted(
                        2,
                        "Entry #%zu of %zu: term=%d, no-op",
                        (size_t)(i + 1),
                        message.log.size(),
                        message.log[i].term
                    );
                } else {
                    diagnosticsSender.SendDiagnosticInformationFormatted(
                        2,
                        "Entry #%zu of %zu: term=%d, command: '%s'",
                        (size_t)(i + 1),
                        message.log.size(),
                        message.log[i].term,
                        message.log[i].command->GetType().c_str()
                    );
                }
            }
        }
#endif /* EXTRA_DIAGNOSTICS */
        Message response;
        response.type = Message::Type::AppendEntriesResults;
        response.term = persistentStateCache.currentTerm;
        response.seq = message.seq;
        if (persistentStateCache.currentTerm > message.term) {
            response.appendEntriesResults.success = false;
            response.appendEntriesResults.matchIndex = 0;
        } else if (
            (electionState == ElectionState::Leader)
            && (persistentStateCache.currentTerm == message.term)
        ) {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                SystemAbstractions::DiagnosticsSender::Levels::ERROR,
                "Received AppendEntries (%zu entries building on %zu from term %d) from server %d in SAME term %d",
                message.log.size(),
                message.appendEntries.prevLogIndex,
                message.appendEntries.prevLogTerm,
                senderInstanceNumber,
                message.term
            );
            return;
        } else {
            bool electionStateChanged = (electionState != ElectionState::Follower);
            if (
                (electionState != ElectionState::Leader)
                || (persistentStateCache.currentTerm < message.term)
            ) {
                if (persistentStateCache.currentTerm < message.term) {
                    electionStateChanged = true;
                }
                UpdateCurrentTerm(message.term);
                if (!thisTermLeaderAnnounced) {
                    thisTermLeaderAnnounced = true;
                    leaderId = senderInstanceNumber;
                    processingMessageFromLeader = true;
                    QueueLeadershipChangeAnnouncement(
                        senderInstanceNumber,
                        persistentStateCache.currentTerm
                    );
                }
            }
            RevertToFollower();
            MeasureTimeBetweenLeaderMessages();
            if (electionStateChanged) {
                QueueElectionStateChangeAnnouncement();
            }
            if (selfCatchUpIndex == 0) {
                selfCatchUpIndex = message.appendEntries.prevLogIndex + message.log.size();
            }
            if (message.appendEntries.leaderCommit > commitIndex) {
                AdvanceCommitIndex(message.appendEntries.leaderCommit);
            }
            if (
                (message.appendEntries.prevLogIndex > lastIndex)
                || (
                    logKeeper->GetTerm(message.appendEntries.prevLogIndex)
                    != message.appendEntries.prevLogTerm
                )
            ) {
                response.appendEntriesResults.success = false;
                if (message.appendEntries.prevLogIndex > lastIndex) {
                    response.appendEntriesResults.matchIndex = lastIndex;
                    diagnosticsSender.SendDiagnosticInformationFormatted(
                        3,
                        "Mismatch in received AppendEntries (%zu > %zu)",
                        message.appendEntries.prevLogIndex,
                        lastIndex
                    );
                } else {
                    response.appendEntriesResults.matchIndex = message.appendEntries.prevLogIndex - 1;
                    diagnosticsSender.SendDiagnosticInformationFormatted(
                        3,
                        "Mismatch in received AppendEntries (%zu <= %zu but %d != %d)",
                        message.appendEntries.prevLogIndex,
                        lastIndex,
                        logKeeper->GetTerm(message.appendEntries.prevLogIndex),
                        message.appendEntries.prevLogTerm
                    );
                }
            } else {
                response.appendEntriesResults.success = true;
                size_t nextIndex = message.appendEntries.prevLogIndex + 1;
                std::vector< LogEntry > entriesToAdd;
                bool conflictFound = false;
                for (size_t i = 0; i < message.log.size(); ++i) {
                    auto& newEntry = message.log[i];
                    const auto logIndex = message.appendEntries.prevLogIndex + i + 1;
                    if (
                        conflictFound
                        || (logIndex > lastIndex)
                    ) {
                        entriesToAdd.push_back(std::move(newEntry));
                    } else {
                        if (logKeeper->GetTerm(logIndex) != newEntry.term) {
                            conflictFound = true;
                            SetLastIndex(logIndex - 1);
                            logKeeper->RollBack(logIndex - 1);
                            entriesToAdd.push_back(std::move(newEntry));
                        }
                    }
                }
                if (!entriesToAdd.empty()) {
                    logKeeper->Append(entriesToAdd);
                }
                SetLastIndex(logKeeper->GetLastIndex());
                response.appendEntriesResults.matchIndex = std::min(
                    lastIndex,
                    message.appendEntries.prevLogIndex + message.log.size()
                );
            }
        }
        const auto now = scheduler->GetClock()->GetCurrentTime();
        timeOfLastLeaderMessage = now;
        SerializeAndQueueMessageToBeSent(response, senderInstanceNumber, now);
    }

    void Server::Impl::OnReceiveAppendEntriesResults(
        Message&& message,
        int senderInstanceNumber
    ) {
        auto& instance = instances[senderInstanceNumber];
        if (
            instance.awaitingResponse
            && (
                (message.seq == 0)
                || (instance.lastSerialNumber == message.seq)
            )
        ) {
            MeasureBroadcastTime(instance.timeLastRequestSent);
        } else {
#ifdef EXTRA_DIAGNOSTICS
            diagnosticsSender.SendDiagnosticInformationFormatted(
                3,
                "Received unexpected (redundant?) AppendEntriesResults(%s, term %d, match %zu, next %zu) from server %d (we are %s in term %d)",
                (message.appendEntriesResults.success ? "success" : "failure"),
                message.term,
                message.appendEntriesResults.matchIndex,
                instance.nextIndex,
                senderInstanceNumber,
                ElectionStateToString(electionState).c_str(),
                persistentStateCache.currentTerm
            );
#endif /* EXTRA_DIAGNOSTICS */
            return;
        }
#ifdef EXTRA_DIAGNOSTICS
        diagnosticsSender.SendDiagnosticInformationFormatted(
            1,
            "Received AppendEntriesResults(%s, term %d, match %zu, next %zu) from server %d (we are %s in term %d)",
            (message.appendEntriesResults.success ? "success" : "failure"),
            message.term,
            message.appendEntriesResults.matchIndex,
            instance.nextIndex,
            senderInstanceNumber,
            ElectionStateToString(electionState).c_str(),
            persistentStateCache.currentTerm
        );
#endif /* EXTRA_DIAGNOSTICS */
        if (message.term > persistentStateCache.currentTerm) {
            UpdateCurrentTerm(message.term);
            RevertToFollower();
            QueueElectionStateChangeAnnouncement();
        }
        if (electionState != ElectionState::Leader) {
            return;
        }
        CancelRetransmissionCallbacks(instance);
        instance.matchIndex = message.appendEntriesResults.matchIndex;
        if (instance.matchIndex > lastIndex) {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                3,
                "Received AppendEntriesResults with match index %zu which is beyond our last index %zu",
                instance.matchIndex,
                lastIndex
            );
            instance.matchIndex = lastIndex;
        }
        instance.nextIndex = instance.matchIndex + 1;
        if (instance.nextIndex <= lastIndex) {
            AttemptLogReplication(senderInstanceNumber);
        }
        std::map< size_t, size_t > indexMatchCountsOldServers;
        std::map< size_t, size_t > indexMatchCountsNewServers;
        for (auto instanceId: GetInstanceIds()) {
            auto& instance = instances[instanceId];
            if (instanceId == serverConfiguration.selfInstanceId) {
                continue;
            }
            if (
                clusterConfiguration.instanceIds.find(instanceId)
                != clusterConfiguration.instanceIds.end()
            ) {
                ++indexMatchCountsOldServers[instance.matchIndex];
            }
            if (
                nextClusterConfiguration.instanceIds.find(instanceId)
                != nextClusterConfiguration.instanceIds.end()
            ) {
                ++indexMatchCountsNewServers[instance.matchIndex];
            }
        }
        size_t totalMatchCounts = 0;
        if (
            clusterConfiguration.instanceIds.find(serverConfiguration.selfInstanceId)
            != clusterConfiguration.instanceIds.end()
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
                (indexMatchCountsOldServersEntry->first > commitIndex)
                && (
                    totalMatchCounts
                    > clusterConfiguration.instanceIds.size() - totalMatchCounts
                )
                && (
                    logKeeper->GetTerm(indexMatchCountsOldServersEntry->first)
                    == persistentStateCache.currentTerm
                )
            ) {
                if (jointConfiguration) {
                    totalMatchCounts = 0;
                    if (
                        nextClusterConfiguration.instanceIds.find(serverConfiguration.selfInstanceId)
                        != nextClusterConfiguration.instanceIds.end()
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
                            (indexMatchCountsNewServersEntry->first > commitIndex)
                            && (
                                totalMatchCounts
                                > nextClusterConfiguration.instanceIds.size() - totalMatchCounts
                            )
                            && (
                                logKeeper->GetTerm(indexMatchCountsNewServersEntry->first)
                                == persistentStateCache.currentTerm
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
        Message&& message,
        int senderInstanceNumber
    ) {
        diagnosticsSender.SendDiagnosticInformationFormatted(
            3,
            "Received InstallSnapshot (%zu entries up to term %d) from server %d in term %d (we are in term %d)",
            message.installSnapshot.lastIncludedIndex,
            message.installSnapshot.lastIncludedTerm,
            senderInstanceNumber,
            message.term,
            persistentStateCache.currentTerm
        );
        Message response;
        response.type = Message::Type::InstallSnapshotResults;
        response.term = persistentStateCache.currentTerm;
        response.seq = message.seq;
        if (persistentStateCache.currentTerm <= message.term) {
            bool electionStateChanged = (electionState != ElectionState::Follower);
            if (electionState != ElectionState::Leader) {
                if (persistentStateCache.currentTerm < message.term) {
                    electionStateChanged = true;
                }
                UpdateCurrentTerm(message.term);
                if (!thisTermLeaderAnnounced) {
                    thisTermLeaderAnnounced = true;
                    leaderId = senderInstanceNumber;
                    processingMessageFromLeader = true;
                    QueueLeadershipChangeAnnouncement(
                        senderInstanceNumber,
                        persistentStateCache.currentTerm
                    );
                }
            }
            RevertToFollower();
            if (electionStateChanged) {
                QueueElectionStateChangeAnnouncement();
            }
            if (commitIndex < message.installSnapshot.lastIncludedIndex) {
                logKeeper->InstallSnapshot(
                    message.snapshot,
                    message.installSnapshot.lastIncludedIndex,
                    message.installSnapshot.lastIncludedTerm
                );
                lastIndex = message.installSnapshot.lastIncludedIndex;
                QueueSnapshotAnnouncement(
                    std::move(message.snapshot),
                    message.installSnapshot.lastIncludedIndex,
                    message.installSnapshot.lastIncludedTerm
                );
            }
            response.installSnapshotResults.matchIndex = lastIndex;
            ResetElectionTimer();
        }
        const auto now = scheduler->GetClock()->GetCurrentTime();
        timeOfLastLeaderMessage = now;
        SerializeAndQueueMessageToBeSent(response, senderInstanceNumber, now);
    }

    void Server::Impl::OnReceiveInstallSnapshotResults(
        Message&& message,
        int senderInstanceNumber
    ) {
        auto& instance = instances[senderInstanceNumber];
        if (
            instance.awaitingResponse
            && (
                (message.seq == 0)
                || (instance.lastSerialNumber == message.seq)
            )
        ) {
            MeasureBroadcastTime(instance.timeLastRequestSent);
        } else {
            diagnosticsSender.SendDiagnosticInformationFormatted(
                3,
                "Received unexpected (redundant?) InstallSnapshotResults(term %d) from server %d (we are %s in term %d)",
                message.term,
                senderInstanceNumber,
                ElectionStateToString(electionState).c_str(),
                persistentStateCache.currentTerm
            );
            return;
        }
        diagnosticsSender.SendDiagnosticInformationFormatted(
            1,
            "Received InstallSnapshotResults(term %d) from server %d (we are %s in term %d)",
            message.term,
            senderInstanceNumber,
            ElectionStateToString(electionState).c_str(),
            persistentStateCache.currentTerm
        );
        if (message.term > persistentStateCache.currentTerm) {
            UpdateCurrentTerm(message.term);
            RevertToFollower();
            QueueElectionStateChangeAnnouncement();
        }
        if (electionState != ElectionState::Leader) {
            return;
        }
        CancelRetransmissionCallbacks(instance);
        instance.matchIndex = message.installSnapshotResults.matchIndex;
        instance.nextIndex = message.installSnapshotResults.matchIndex + 1;
        if (instance.nextIndex <= lastIndex) {
            AttemptLogReplication(senderInstanceNumber);
        }
        // TODO: This really needs refactoring!
        // * Ugly code!
        // * Two instances of this code.
        std::map< size_t, size_t > indexMatchCountsOldServers;
        std::map< size_t, size_t > indexMatchCountsNewServers;
        for (auto instanceId: GetInstanceIds()) {
            auto& instance = instances[instanceId];
            if (instanceId == serverConfiguration.selfInstanceId) {
                continue;
            }
            if (
                clusterConfiguration.instanceIds.find(instanceId)
                != clusterConfiguration.instanceIds.end()
            ) {
                ++indexMatchCountsOldServers[instance.matchIndex];
            }
            if (
                nextClusterConfiguration.instanceIds.find(instanceId)
                != nextClusterConfiguration.instanceIds.end()
            ) {
                ++indexMatchCountsNewServers[instance.matchIndex];
            }
        }
        size_t totalMatchCounts = 0;
        if (
            clusterConfiguration.instanceIds.find(serverConfiguration.selfInstanceId)
            != clusterConfiguration.instanceIds.end()
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
                (indexMatchCountsOldServersEntry->first > commitIndex)
                && (
                    totalMatchCounts
                    > clusterConfiguration.instanceIds.size() - totalMatchCounts
                )
                && (
                    logKeeper->GetTerm(indexMatchCountsOldServersEntry->first)
                    == persistentStateCache.currentTerm
                )
            ) {
                if (jointConfiguration) {
                    totalMatchCounts = 0;
                    if (
                        nextClusterConfiguration.instanceIds.find(serverConfiguration.selfInstanceId)
                        != nextClusterConfiguration.instanceIds.end()
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
                            (indexMatchCountsNewServersEntry->first > commitIndex)
                            && (
                                totalMatchCounts
                                > nextClusterConfiguration.instanceIds.size() - totalMatchCounts
                            )
                            && (
                                logKeeper->GetTerm(indexMatchCountsNewServersEntry->first)
                                == persistentStateCache.currentTerm
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

    void Server::Impl::EventQueueWorker() {
        std::unique_lock< decltype(eventQueueMutex) > lock(eventQueueMutex);
        diagnosticsSender.SendDiagnosticInformationString(
            0,
            "Event queue worker thread started"
        );
        while (!stopEventQueueWorker) {
            eventQueueWorkerWakeCondition.wait(
                lock,
                [this]{
                    return (
                        stopEventQueueWorker
                        || !eventQueue.IsEmpty()
                    );
                }
            );
            ProcessEventQueue(lock);
        }
        diagnosticsSender.SendDiagnosticInformationString(
            0,
            "Event queue worker thread stopping"
        );
    }

}
