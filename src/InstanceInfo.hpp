#ifndef RAFT_INSTANCE_INFO_HPP
#define RAFT_INSTANCE_INFO_HPP

/**
 * @file InstanceInfo.hpp
 *
 * This module contains the declaration of the Raft::InstanceInfo structure.
 *
 * © 2019 by Richard Walters
 */

#include <stddef.h>
#include <string>

namespace Raft {

    /**
     * This holds information that one server holds about another server.
     */
    struct InstanceInfo {
        /**
         * This indicates whether or not we're awaiting a response to the
         * last RPC call message sent to this instance.
         */
        bool awaitingResponse = false;

        /**
         * This is the time, according to the time keeper, that a request was
         * last sent to the instance.
         */
        double timeLastRequestSent = 0.0;

        /**
         * This is the last request sent to the instance.
         */
        std::string lastRequest;

        /**
         * This is the index of the next log entry to send to this server.
         */
        size_t nextIndex = 0;

        /**
         * This is the index of the highest log entry known to be replicated
         * on this server.
         */
        size_t matchIndex = 0;
    };

}

#endif /* RAFT_INSTANCE_INFO_HPP */
