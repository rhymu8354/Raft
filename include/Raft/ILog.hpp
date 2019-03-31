#ifndef RAFT_I_LOG_HPP
#define RAFT_I_LOG_HPP

/**
 * @file ILog.hpp
 *
 * This module declares the Raft::ILog interface.
 *
 * © 2018 by Richard Walters
 */

#include "LogEntry.hpp"

#include <stddef.h>
#include <vector>

namespace Raft {

    /**
     * This is the interface that a Server needs in order to access the log
     * entries which are replicated across the server cluster.
     */
    class ILog {
    public:
        /**
         * Return the number of entries in the log, including entries that have
         * been incorporated into the snapshot and are no longer available as
         * individual entries for replication.
         *
         * @return
         *     The number of entries in the log is returned.
         */
        virtual size_t GetSize() = 0;

        /**
         * Return the last index represented by the snapshot upon which this
         * log is based.
         *
         * @return
         *     The last index represented by the snapshot upon which this
         *     log is based is returned.
         */
        virtual size_t GetBaseIndex() = 0;

        /**
         * Return the index of the last entry in the log, or in the snapshot
         * upon which this log is based, if the log is empty.
         *
         * @return
         *     The index of the last entry in the log, or in the snapshot
         *     upon which this log is based, if the log is empty, is returned.
         */
        virtual size_t GetLastIndex() = 0;

        /**
         * Return the term of the log entry at the given index.
         *
         * @param[in] index
         *     This is the index of the log entry for which to return the term.
         *
         * @return
         *     The term of the log entry at the given index is returned.
         */
        virtual int GetTerm(size_t index) = 0;

        /**
         * Return the log entry at the given index.
         *
         * @param[in] index
         *     This is the index of the log entry to return.
         *
         * @return
         *     The requested log entry is returned.
         */
        virtual const Raft::LogEntry& operator[](size_t index) = 0;

        /**
         * Discard all log entries after the one at the given index.
         *
         * @param[in] index
         *     This is the index of the last log entry to keep.
         */
        virtual void RollBack(size_t index) = 0;

        /**
         * Append the given entries to the log.
         *
         * @param[in] entries
         *     These are the entries to append to the log.
         */
        virtual void Append(const std::vector< Raft::LogEntry >& entries) = 0;

        /**
         * Let the log know that all entries up to and including the given
         * entry have been replicated to a majority of servers in the cluster,
         * and so can be applied to the server state.
         *
         * It's possible the given index, and/or indices beyond this, may
         * already be committed.  In this case, the log is expected to handle
         * this by doing nothing.
         *
         * @param[in] index
         *     This is the index of the last entry in the log that a majority
         *     of servers in the cluster have successfully stored.
         */
        virtual void Commit(size_t index) = 0;
    };

}

#endif /* RAFT_I_LOG_HPP */
