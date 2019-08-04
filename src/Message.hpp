#ifndef RAFT_MESSAGE_HPP
#define RAFT_MESSAGE_HPP

/**
 * @file Message.hpp
 *
 * This module declares the Raft::Message structure.
 *
 * © 2018 by Richard Walters
 */

#include <memory>
#include <Json/Value.hpp>
#include <Raft/LogEntry.hpp>
#include <stddef.h>
#include <string>
#include <vector>

namespace Raft {

    /**
     * This is the base class for a message sent from one Message to another
     * within the cluster.
     */
    struct Message {
        // Types

        /**
         * These are the types of messages for which the message object might
         * be used.
         */
        enum class Type {
            /**
             * This is the default type of message, used for uninitialized
             * messages.
             */
            Unknown,

            /**
             * This identifies the message as being a "RequestVote RPC" meaning
             * the server is starting an election and voting for itself.
             */
            RequestVote,

            /**
             * This identifies the message as being a "RequestVote RPC results"
             * meaning the server is responding to a "RequestVote RPC" message,
             * where another server started an election.
             */
            RequestVoteResults,

            /**
             * This is sent by the cluster leader to either replicate log
             * entries or send a "heartbeat" to prevent followers from starting
             * new elections.
             */
            AppendEntries,

            /**
             * This is sent by a follower to respond to an AppendEntries
             * message.
             */
            AppendEntriesResults,

            /**
             * This is sent by the cluster leader to replicate server state
             * that has been condensed into a snapshot.
             */
            InstallSnapshot,

            /**
             * This is sent by a follower to respond to an InstallSnapshot
             * message.
             */
            InstallSnapshotResults,
        };

        /**
         * This holds message properties for RequestVote type messages.
         */
        struct RequestVoteDetails {
            /**
             * This is the instance ID of the candidate requesting the vote.
             */
            int candidateId = 0;

            /**
             * This is the index of the last entry that was appended to the
             * candidate's log.
             */
            size_t lastLogIndex = 0;

            /**
             * This is the term of the last entry that was appended to the
             * candidate's log.
             */
            int lastLogTerm = 0;
        };

        /**
         * This holds message properties for RequestVoteResults type messages.
         */
        struct RequestVoteResultsDetails {
            /**
             * This is true if the sender granted their vote to the candidate.
             */
            bool voteGranted = false;
        };

        /**
         * This holds message properties for AppendEntries type messages.
         */
        struct AppendEntriesDetails {
            /**
             * This is the index of the last log entry that the leader knows
             * has been successfully replicated to a majority of servers in the
             * cluster.
             */
            size_t leaderCommit = 0;

            /**
             * This is the index of the log entry that comes just before the
             * entries included with this message.
             */
            size_t prevLogIndex = 0;

            /**
             * This is the term of the log entry that comes just before the
             * entries included with this message.
             */
            int prevLogTerm = 0;
        };

        /**
         * This holds message properties for AppendEntriesResults type
         * messages.
         */
        struct AppendEntriesResultsDetails {
            /**
             * This indicates whether or not the server accepted the last
             * AppendEntries message.
             */
            bool success = false;

            /**
             * This is the index of the last log entry which the follower
             * has determined matches what the leader has.
             */
            size_t matchIndex = 0;
        };

        /**
         * This holds message properties for InstallSnapshot type messages.
         */
        struct InstallSnapshotDetails {
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
        };

        /**
         * This holds message properties for InstallSnapshotResults type
         * messages.
         */
        struct InstallSnapshotResultsDetails {
            /**
             * This is the index of the last log entry which the follower
             * has determined matches what the leader has.
             */
            size_t matchIndex = 0;
        };

        // Properties

        /**
         * This indicates for what purpose the message is being sent.
         */
        Type type = Type::Unknown;

        /**
         * This is the current term in effect at the sender.
         */
        int term = 0;

        /**
         * This is the sequence number used to match responses with
         * their original requests.
         */
        int seq = 0;

        /**
         * This holds properties specific to each type of message.
         */
        union {
            RequestVoteDetails requestVote;
            RequestVoteResultsDetails requestVoteResults;
            AppendEntriesDetails appendEntries;
            AppendEntriesResultsDetails appendEntriesResults;
            InstallSnapshotDetails installSnapshot;
            InstallSnapshotResultsDetails installSnapshotResults;
        };

        /**
         * These are the log entries attached to the message.
         *
         * This is only used for messages of type AppendEntries.
         */
        std::vector< LogEntry > log;

        /**
         * This is a condensed form of all the information the server needs
         * to reconstruct its state as built from all log entries from the
         * beginning up to and including the entry with index of
         * `lastIncludedIndex`.
         *
         * This is only used for messages of type InstallSnapshot.
         */
        Json::Value snapshot;

        // Methods

        /**
         * This is the constructor of the class.
         *
         * @param[in] serialization
         *     If not empty, this is the serialized form of the message, used
         *     to initialize the type and properties of the message.
         */
        Message(const std::string& serialization = "");

        /**
         * This method returns a string which can be used to construct a new
         * message with the exact same contents as this message.
         *
         * @return
         *     A string which can be used to construct a new message with the
         *     exact same contents as this message is returned.
         */
        std::string Serialize() const;
    };

}

#endif /* RAFT_MESSAGE_HPP */
