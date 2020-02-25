#pragma once

/**
 * @file IPersistentState.hpp
 *
 * This module declares the Raft::IPersistentState interface.
 *
 * © 2018-2020 by Richard Walters
 */

namespace Raft {

    /**
     * This is the interface that a Server needs in order to make some of its
     * state variables persistent.
     */
    class IPersistentState {
    public:
        // Types

        /**
         * This holds the state variables of the server that need to be
         * persistent.
         */
        struct Variables {
            /**
             * This is the last term the server has seen.
             */
            int currentTerm = 0;

            /**
             * If the server has voted for another server to be the leader this
             * term, this is the unique identifier of the server for whom we
             * voted.
             */
            int votedFor = 0;

            /**
             * This indicates whether or not the server has voted for another
             * server to be the leader this term.
             */
            bool votedThisTerm = false;
        };

        // Methods

        /**
         * Load the state variables from persistent storage.
         *
         * @return
         *     The persistent state variables of the server are returned.
         *
         * @retval Variables()
         *     A default Variables object is returned if no state variables
         *     could be loaded from persistent storage.
         *
         */
        virtual Variables Load() = 0;

        /**
         * Save the state variables to persistent storage.
         *
         * @param[in] variables
         *     This contains the server state variables to save in persistent
         *     storage.
         */
        virtual void Save(const Variables& variables) = 0;
    };

}
