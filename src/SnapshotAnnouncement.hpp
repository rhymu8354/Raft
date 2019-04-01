#pragma once

/**
 * @file SnapshotAnnouncement.hpp
 *
 * This module contains the declaration of the
 * Raft::SnapshotAnnouncement structure.
 *
 * © 2019 by Richard Walters
 */

#include <Json/Value.hpp>
#include <Raft/ClusterConfiguration.hpp>
#include <stddef.h>

namespace Raft {

    /**
     * This holds information used to store a snapshot
     * announcement to be sent later.
     */
    struct SnapshotAnnouncement {
        /**
         * This contains a complete copy of the server state, built from the
         * first log entry up to and including the entry at the given last
         * included index.
         */
        Json::Value snapshot;

        /**
         * This is the index of the last log entry that was used to assemble
         * the snapshot.
         */
        size_t lastIncludedIndex = 0;

        /**
         * This is the term of the last log entry that was used to assemble the
         * snapshot.
         */
        int lastIncludedTerm = 0;
    };

}
