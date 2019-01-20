#ifndef ELECTION_STATE_CHANGE_ANNOUNCEMENT_HPP
#define ELECTION_STATE_CHANGE_ANNOUNCEMENT_HPP

/**
 * @file ElectionStateChangeAnnouncement.hpp
 *
 * This module contains the declaration of the
 * Raft::ElectionStateChangeAnnouncement structure.
 *
 * © 2019 by Richard Walters
 */

#include <Raft/IServer.hpp>

namespace Raft {

    /**
     * This holds information used to store an election state change
     * announcement to be sent later.
     */
    struct ElectionStateChangeAnnouncement {
        /**
         * This is the generation number of the server cluster leadership,
         * which is incremented whenever a new election is started.
         */
        int term = 0;

        /**
         * This indicates whether the server is currently a follower,
         * candidate, or leader.
         */
        IServer::ElectionState electionState = IServer::ElectionState::Follower;

        /**
         * This indicates whether or not the server voted for a candidate
         * in this term.
         */
        bool didVote = false;

        /**
         * This is the unique identifier of the server for which this
         * server voted in this term, if a vote was indeed cast.  It will
         * be zero if the server did not vote for a candidate
         * in this term.
         */
        int votedFor = 0;
    };

}

#endif /* ELECTION_STATE_CHANGE_ANNOUNCEMENT_HPP */
