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
            Election,
        };

        /**
         * This holds message properties for Election type messages.
         */
        struct ElectionDetails {
            /**
             * This is the term of the new election.
             */
            unsigned int term = 0;

            /**
             * This is the instance ID of the candidate requesting the vote.
             */
            unsigned int candidateId = 0;
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
            ElectionDetails election;
        };

        // Methods

        /**
         * This is the default constructor of the object.
         */
        MessageImpl();

    };

}
