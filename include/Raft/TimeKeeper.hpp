#ifndef RAFT_TIME_KEEPER_HPP
#define RAFT_TIME_KEEPER_HPP

/**
 * @file TimeKeeper.hpp
 *
 * This module declares the Raft::TimeKeeper interface.
 *
 * © 2018 by Richard Walters
 */

namespace Raft {

    /**
     * This represents the time-keeping requirements of Raft components.
     * To integrate Raft components together, implement this
     * interface in terms of the actual server time.
     */
    class TimeKeeper {
    public:
        // Methods

        /**
         * This method returns the current server time, in seconds.
         *
         * @return
         *     The current server time is returned, in seconds.
         */
        virtual double GetCurrentTime() = 0;
    };

}

#endif /* RAFT_TIME_KEEPER_HPP */
