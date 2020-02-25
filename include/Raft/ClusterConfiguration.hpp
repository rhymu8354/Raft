#pragma once

/**
 * @file ClusterConfiguration.hpp
 *
 * This module declares the Raft::ClusterConfiguration structure.
 *
 * © 2018-2020 by Richard Walters
 */

#include <set>

namespace Raft {

    /**
     * This holds the properties which make up the configuration of the
     * overall server cluster.
     */
    struct ClusterConfiguration {
        /**
         * This holds the unique identifiers of all servers in the cluster.
         */
        std::set< int > instanceIds;
    };

}
