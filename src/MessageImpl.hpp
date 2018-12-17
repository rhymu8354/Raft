/**
 * @file MessageImpl.hpp
 *
 * This module contains the declaration of the package-private
 * Raft::MessageImpl structure.
 *
 * © 2018 by Richard Walters
 */

namespace Raft {

    /**
     * This contains the package-private properties of a Message class
     * instance.
     */
    struct MessageImpl {
        // Types

        /**
         * These are the types of messages for which the message object might
         * be used.
         */
        enum class Type {
            /**
             * This is the default type of message, used for messages outside
             * the scope of Raft.
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
             * This is sent by leader periodically to prevent followers from
             * starting new elections.
             */
            HeartBeat,
        };

        /**
         * This holds message properties for RequestVote type messages.
         */
        struct RequestVoteDetails {
            /**
             * This is the term of the new election.
             */
            int term = 0;

            /**
             * This is the instance ID of the candidate requesting the vote.
             */
            int candidateId = 0;
        };

        /**
         * This holds message properties for RequestVoteResults type messages.
         */
        struct RequestVoteResultsDetails {
            /**
             * This is the current term in effect at the sender.
             */
            int term = 0;

            /**
             * This is true if the sender granted their vote to the candidate.
             */
            bool voteGranted = false;
        };

        /**
         * This holds message properties for Heartbeat type messages.
         */
        struct HeartbeatDetails {
            /**
             * This is the current term in effect at the sender.
             */
            int term = 0;
        };

        // Properties

        /**
         * This indicates for what purpose the message is being sent.
         */
        Type type = Type::Unknown;

        /**
         * This holds properties specific to each type of message.
         */
        union {
            RequestVoteDetails requestVote;
            RequestVoteResultsDetails requestVoteResults;
            HeartbeatDetails heartbeat;
        };

        // Methods

        /**
         * This is the default constructor of the object.
         */
        MessageImpl();

    };

}
