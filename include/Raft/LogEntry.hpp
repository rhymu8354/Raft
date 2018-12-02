#ifndef RAFT_LOG_ENTRY_HPP
#define RAFT_LOG_ENTRY_HPP

/**
 * @file LogEntry.hpp
 *
 * This module declares the Raft::LogEntry implementation.
 *
 * © 2018 by Richard Walters
 */

namespace Raft {

    /**
     * This is the base class for log entries of a server which uses the Raft
     * Consensus Algorithm.  It contains all the properties and methods which
     * directly relate to the algorithm.  It is meant to be subclassed in order
     * to hold actual concrete server state.
     */
    class LogEntry {
    };

}

#endif /* RAFT_LOG_ENTRY_HPP */
