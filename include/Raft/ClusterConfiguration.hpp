#ifndef RAFT_CLUSTER_CONFIGURATION_HPP
#define RAFT_CLUSTER_CONFIGURATION_HPP

/**
 * @file ClusterConfiguration.hpp
 *
 * This module declares the Raft::ClusterConfiguration structure.
 *
 * © 2018 by Richard Walters
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

#endif /* RAFT_CLUSTER_CONFIGURATION_HPP */
