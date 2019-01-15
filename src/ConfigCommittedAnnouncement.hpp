#ifndef RAFT_CONFIG_COMMITTED_ANNOUNCEMENT_HPP
#define RAFT_CONFIG_COMMITTED_ANNOUNCEMENT_HPP

/**
 * @file ConfigCommittedAnnouncement.hpp
 *
 * This module contains the declaration of the
 * Raft::ConfigCommittedAnnouncement structure.
 *
 * © 2019 by Richard Walters
 */

#include <Raft/ClusterConfiguration.hpp>
#include <stddef.h>

namespace Raft {

    /**
     * This holds information used to store a configuration committed
     * announcement to be sent later.
     */
    struct ConfigCommittedAnnouncement {
        /**
         * This is the new single cluster configuration committed by
         * the cluster.
         */
        Raft::ClusterConfiguration newConfig;

        /**
         * This is the index of the log at the point where the cluster
         * configuration was committed.
         */
        size_t logIndex = 0;
    };

}

#endif /* RAFT_CONFIG_COMMITTED_ANNOUNCEMENT_HPP */
