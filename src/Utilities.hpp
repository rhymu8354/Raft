#ifndef RAFT_UTILITIES_HPP
#define RAFT_UTILITIES_HPP

/**
 * @file Utilities.hpp
 *
 * This module contains the declaration of free functions used by other parts
 * of the library implementation.
 *
 * © 2019 by Richard Walters
 */

#include <Raft/IServer.hpp>
#include <set>
#include <string>
#include <sstream>

namespace Raft {

    /**
     * This is a support function for Google Test to print out
     * values of the Raft::IServer::ElectionState type.
     *
     * @param[in] electionState
     *     This is the election state value to print.
     *
     * @param[in] os
     *     This points to the stream to which to print the
     *     election state value.
     */
    void PrintTo(
        const IServer::ElectionState& electionState,
        std::ostream* os
    );

    /**
     * Return a human-readable string representation of the given server
     * election state.
     *
     * @param[in] electionState
     *     This is the election state to turn into a string.
     *
     * @return
     *     A human-readable string representation of the given server
     *     election state is returned.
     */
    std::string ElectionStateToString(IServer::ElectionState electionState);

    /**
     * This is the template for a function which builds and returns a
     * human-readable string representation of a set of elements.
     *
     * @param[in] s
     *     This is the set of elements to format as a string.
     *
     * @return
     *     A human-readable representation of the given set is returned.
     */
    template< typename T > std::string FormatSet(const std::set< T >& s) {
        std::ostringstream builder;
        builder << '{';
        bool first = true;
        for (const auto& element: s) {
            if (!first) {
                builder << ", ";
            }
            first = false;
            builder << element;
        }
        builder << '}';
        return builder.str();
    }

}

#endif /* RAFT_UTILITIES_HPP */
