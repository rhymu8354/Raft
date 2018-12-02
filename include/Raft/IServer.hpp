#ifndef RAFT_I_SERVER_HPP
#define RAFT_I_SERVER_HPP

/**
 * @file IServer.hpp
 *
 * This module declares the Raft::IServer interface.
 *
 * © 2018 by Richard Walters
 */

#include <memory>
#include <vector>

namespace Raft {

    /**
     * This is the interface to the Server component, which represents one
     * member of the server cluster.
     */
    class IServer {
        // Types
    public:
        /**
         * This is used to instruct the server on how it should configure
         * itself.
         */
        struct Configuration {
            /**
             * This holds the unique identifiers of all servers in the cluster.
             */
            std::vector< unsigned int > instanceNumbers;

            /**
             * This is the unique identifier of this server, amongst all the
             * servers in the cluster.
             */
            unsigned int selfInstanceNumber = 0;
        };

        // Methods
    public:
        /**
         * This method changes the configuration of the server.
         *
         * @param[in] configuration
         *     This holds the configuration items for the server.
         *
         * @return
         *     An indication of whether or not the configuration
         *     was successfully set is returned.
         */
        virtual bool Configure(const Configuration& configuration) = 0;
    };

}

#endif /* RAFT_I_SERVER_HPP */
