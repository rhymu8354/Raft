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
         * Return the number of entries in the log.
         *
         * @return
         *     The number of entries in the log is returned.
         */
        virtual size_t GetSize() = 0;

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
    };

}

#endif /* RAFT_I_LOG_HPP */
