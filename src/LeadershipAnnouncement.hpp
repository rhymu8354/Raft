#ifndef RAFT_LEADERSHIP_ANNOUNCEMENT_HPP
#define RAFT_LEADERSHIP_ANNOUNCEMENT_HPP

/**
 * @file LeadershipAnnouncement.hpp
 *
 * This module contains the declaration of the Raft::LeadershipAnnouncement
 * structure.
 *
 * © 2019 by Richard Walters
 */

namespace Raft {

    /**
     * This holds information used to store a leadership announcement to be
     * sent later.
     */
    struct LeadershipAnnouncement {
        /**
         * This is the unique identifier of the server which has become
         * the leader of the cluster.
         */
        int leaderId = 0;

        /**
         * This is the generation number of the server cluster leadership,
         * which is incremented whenever a new election is started.
         */
        int term = 0;
    };

}

#endif /* RAFT_LEADERSHIP_ANNOUNCEMENT_HPP */
