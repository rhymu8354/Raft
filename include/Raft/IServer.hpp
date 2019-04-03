#ifndef RAFT_I_SERVER_HPP
#define RAFT_I_SERVER_HPP

/**
 * @file IServer.hpp
 *
 * This module declares the Raft::IServer interface.
 *
 * © 2018 by Richard Walters
 */

#include "ClusterConfiguration.hpp"
#include "ILog.hpp"
#include "IPersistentState.hpp"
#include "LogEntry.hpp"

#include <functional>
#include <memory>
#include <ostream>
#include <set>
#include <stddef.h>
#include <string>
#include <vector>

namespace Raft {

    /**
     * This is the interface to the Server component, which represents one
     * member of the server cluster.
     */
    class IServer {
        // Types
    public:
        /**
         * This is used to indicate whether the server is a currently a leader,
         * candidate, or follower in the current election term of the cluster.
         */
        enum class ElectionState {
            /**
             * In this state, the server is neither a leader, or is running for
             * election in the current term.  It is awaiting heartbeat messages
             * from the leader, and if none is received before the election
             * timeout, it will start a new election.
             */
            Follower,

            /**
             * In this state, the server is running for election in the current
             * term, awaiting vote responses.  If it receives a majority vote,
             * it will immediately become the leader.  If it receives a
             * heartbeat, it will immediately become a follower.  Otherwise, it
             * will start a new election once the election timeout occurs.
             */
            Candidate,

            /**
             * In this state, the server is the leader of the cluster in the
             * current term, and will send out heartbeats to all the other
             * servers.  It reverts to a follower if it receives a heartbeat or
             * vote request in a newer term.
             */
            Leader,
        };

        /**
         * This holds the properties which make up the configuration for just
         * this server and not any other servers in the cluster.
         */
        struct ServerConfiguration {
            /**
             * This is the unique identifier of this server, amongst all the
             * servers in the cluster.
             */
            int selfInstanceId = 0;

            /**
             * This is the lower bound of the range of time, starting from the
             * last time the server either started or received a message from
             * the cluster leader, within which to trigger an election.
             */
            double minimumElectionTimeout = 0.15;

            /**
             * This is the upper bound of the range of time, starting from the
             * last time the server either started or received a message from
             * the cluster leader, within which to trigger an election.
             */
            double maximumElectionTimeout = 0.3;

            /**
             * This is the amount of time that the leader will not send any
             * messages before it decides to send out a "heartbeat" message
             * to all followers in order to maintain leadership.
             *
             * It should be greater than the RPC timeout and less than
             * the minimum election timeout.
             */
            double heartbeatInterval = 0.075;

            /**
             * This is the maximum amount of time to wait for a response to an
             * RPC request, before retransmitting the request.
             */
            double rpcTimeout = 0.015;
        };

        /**
         * This is the base type of any event published by the server.
         * All events will subclass this type and set the appropriate value
         * for the type field.
         */
        struct Event {
            /**
             * This is used to identify the subclass of the concrete event.
             */
            const enum class Type {
                /**
                 * This indicates the event is a request that a message be sent
                 * to another server in the cluster.
                 */
                SendMessage,

                /**
                 * This indicates the event announces leadership
                 * changes in the server cluster.
                 */
                LeadershipChange,

                /**
                 * This indicates the event announces the server's
                 * election state changes.
                 */
                ElectionState,

                /**
                 * This indicates the event announces that a single cluster
                 * configuration has been applied by the server.
                 */
                ApplyConfiguration,

                /**
                 * This indicates the event announces that a single cluster
                 * configuration has been committed by the cluster.
                 */
                CommitConfiguration,

                /**
                 * This indicates the event announces that the server has
                 * received a message installing a snapshot to set the server's
                 * state.
                 */
                SnapshotInstalled,

                /**
                 * This indicates the event announces that the server has
                 * "caught up" to the rest of the cluster (the commit index has
                 * reached the initial "last index" of the leader).
                 */
                CaughtUp,
            } type;

            /**
             * This is the constructor of the event.
             *
             * @param[in] type
             *     This is used to identify the subclass of the concrete event.
             */
            explicit Event(Type type) : type(type) {}
        };

        /**
         * This is an event published by the server.  It requests that a
         * message be sent to another server in the cluster.
         */
        struct SendMessageEvent : public Event {
            /**
             * This is the serialized message to send.
             */
            std::string serializedMessage;

            /**
             * This is the unique identifier of the server to whom to send the
             * message.
             */
            int receiverInstanceNumber = 0;

            /**
             * This is the default constructor.
             */
            SendMessageEvent()
                : Event(Type::SendMessage)
            {
            }
        };

        /**
         * This is an event published by the server.  It announces leadership
         * changes in the server cluster.
         */
        struct LeadershipChangeEvent : public Event {
            /**
             * This is the unique identifier of the server which has become the
             * leader of the cluster.
             */
            int leaderId = 0;

            /**
             * This is the generation number of the server cluster leadership,
             * which is incremented whenever a new election is started.
             */
            int term = 0;

            /**
             * This is the default constructor.
             */
            LeadershipChangeEvent()
                : Event(Type::LeadershipChange)
            {
            }
        };

        /**
         * This is an event published by the server.  It announces the server's
         * election state changes.
         */
        struct ElectionStateEvent : public Event {
            /**
             * This is the generation number of the server cluster leadership,
             * which is incremented whenever a new election is started.
             */
            int term = 0;

            /**
             * This indicates whether the server is currently a follower,
             * candidate, or leader.
             */
            ElectionState electionState = ElectionState::Follower;

            /**
             * This indicates whether or not the server voted for a candidate
             * in this term.
             */
            bool didVote = false;

            /**
             * This is the unique identifier of the server for which this
             * server voted in this term, if a vote was indeed cast.  It will
             * be zero if the server did not vote for a candidate in this term.
             */
            int votedFor = 0;

            /**
             * This is the default constructor.
             */
            ElectionStateEvent()
                : Event(Type::ElectionState)
            {
            }
        };

        /**
         * This is an event published by the server.  It announces that a
         * single cluster configuration has been applied by the server.
         */
        struct ApplyConfigurationEvent : public Event {
            /**
             * This is the new single cluster configuration applied by the
             * server.
             */
            ClusterConfiguration newConfig;

            /**
             * This is the default constructor.
             */
            ApplyConfigurationEvent()
                : Event(Type::ApplyConfiguration)
            {
            }
        };

        /**
         * This is an event published by the server.  It announces that a
         * single cluster configuration has been committed by the cluster.
         */
        struct CommitConfigurationEvent : public Event {
            /**
             * This is the new single cluster configuration committed by the
             * cluster.
             */
            ClusterConfiguration newConfig;

            /**
             * This is the index of the log at the point where the cluster
             * configuration was committed.
             */
            size_t logIndex = 0;

            /**
             * This is the default constructor.
             */
            CommitConfigurationEvent()
                : Event(Type::CommitConfiguration)
            {
            }
        };

        /**
         * This is an event published by the server.  It announces that the
         * server has received a message installing a snapshot to set the
         * server's state.
         */
        struct SnapshotInstalledEvent : public Event {
            /**
             * This contains a complete copy of the server state, built from
             * the first log entry up to and including the entry at the given
             * last included index.
             */
            Json::Value snapshot;

            /**
             * This is the index of the last log entry that was used to
             * assemble the snapshot.
             */
            size_t lastIncludedIndex = 0;

            /**
             * This is the term of the last log entry that was used to
             * assemble the snapshot.
             */
            int lastIncludedTerm = 0;

            /**
             * This is the default constructor.
             */
            SnapshotInstalledEvent()
                : Event(Type::SnapshotInstalled)
            {
            }
        };

        /**
         * This is an event published by the server.  It announces that the
         * server has "caught up" to the rest of the cluster (the commit index
         * has reached the initial "last index" of the leader).
         */
        struct CaughtUpEvent : public Event {
            /**
             * This is the default constructor.
             */
            CaughtUpEvent()
                : Event(Type::CaughtUp)
            {
            }
        };

        /**
         * Declare the type of delegate used to deliver events published by the
         * server.
         *
         * @param[in] baseEvent
         *     This is a reference to the base of the event that was published.
         *     The delegate should look at the event's type and downcast
         *     the reference to the matching subtype for more details.
         */
        using EventDelegate = std::function<
            void(
                const Raft::IServer::Event& baseEvent
            )
        >;

        /**
         * Declare the type of delegate returned when a subscriber subscribes
         * to server events.  When called, this delegate cancels the
         * subscription.
         */
        using EventsUnsubscribeDelegate = std::function< void() >;

        // Methods
    public:
        /**
         * Subscribe to events published by the server.
         *
         * @param[in] eventDelegate
         *     This is the delegate to be called whenever an event
         *     is published by the server.
         *
         * @return
         *     A delegate that can be called to cancel the subscription
         *     is returned.
         */
        virtual EventsUnsubscribeDelegate SubscribeToEvents(EventDelegate eventDelegate) = 0;

        /**
         * This method starts the server's worker thread.
         *
         * @param[in] logKeeper
         *     This is the object which is responsible for keeping
         *     the actual log and making it persistent.
         *
         * @param[in] persistentState
         *     This is the object which is responsible for keeping
         *     the server state variables which need to be persistent.
         *
         * @param[in] clusterConfiguration
         *     This holds the configuration items for the cluster.
         *
         * @param[in] serverConfiguration
         *     This holds the configuration items for the server.
         */
        virtual void Mobilize(
            std::shared_ptr< ILog > logKeeper,
            std::shared_ptr< IPersistentState > persistentStateKeeper,
            const ClusterConfiguration& clusterConfiguration,
            const ServerConfiguration& serverConfiguration
        ) = 0;

        /**
         * This method stops the server's worker thread.
         */
        virtual void Demobilize() = 0;

        /**
         * This method is called whenever the server receives a message
         * from another server in the cluster.
         *
         * @param[in] serializedMessage
         *     This is the serialized message received from another server
         *     in the cluster.
         *
         * @param[in] senderInstanceNumber
         *     This is the unique identifier of the server that sent the
         *     message.
         */
        virtual void ReceiveMessage(
            const std::string& serializedMessage,
            int senderInstanceNumber
        ) = 0;

        /**
         * This method returns an indication of whether the server is currently
         * the leader, a candidate, or a follower in the current term of the
         * cluster.
         *
         * @return
         *     An indication of whether the server is currently the leader,
         *     a candidate, or a follower in the current term of the cluster
         *     is returned.
         */
        virtual ElectionState GetElectionState() = 0;

        /**
         * Add the given entries to the server log.  Send out AppendEntries
         * messages to the rest of the server cluster in order to replicate the
         * log entries.
         *
         * @param[in] entries
         *     These are the log entries to be added and replicated on all
         *     servers in the cluster.
         */
        virtual void AppendLogEntries(const std::vector< LogEntry >& entries) = 0;

        /**
         * Begin the process of reconfiguring the server cluster.  The server
         * is expected to start sending log entries to the union of servers in
         * both the current and new configurations.  Once all "new servers"
         * have "caught up", the server will go through the joint configuration
         * process to transition to the new configuration.
         *
         * @param[in] newConfiguration
         *     This represents the cluster shape to which we want to
         *     transition.
         */
        virtual void ChangeConfiguration(const ClusterConfiguration& newConfiguration) = 0;

        /**
         * Reset collected statistics.
         */
        virtual void ResetStatistics() = 0;

        /**
         * Return collected statistics.
         *
         * @return
         *     Statistics collected by the server are returned.
         */
        virtual Json::Value GetStatistics() = 0;
    };

    /**
     * This is a support function for Google Test to print out
     * values of the Raft::IServer::ElectionState class.
     *
     * @param[in] electionState
     *     This is the Raft election state value to print.
     *
     * @param[in] os
     *     This points to the stream to which to print the
     *     Raft election state value.
     */
    void PrintTo(
        const Raft::IServer::ElectionState& electionState,
        std::ostream* os
    );

}

#endif /* RAFT_I_SERVER_HPP */
