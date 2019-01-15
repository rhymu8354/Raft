/**
 * @file ServerSharedProperties.cpp
 *
 * This module contains the implementation of the Raft::ServerSharedProperties
 * structure.
 *
 * © 2019 by Richard Walters
 */

#include "ServerSharedProperties.hpp"

namespace Raft {

    ServerSharedProperties::ServerSharedProperties()
        : diagnosticsSender("Raft::Server")
    {
    }

}
