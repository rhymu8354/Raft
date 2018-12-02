/**
 * @file MessageImpl.hpp
 *
 * This module contains the declaration of the package-private
 * Raft::MessageImpl structure.
 *
 * © 2018 by Richard Walters
 */

namespace Raft {

    /**
     * This contains the package-private properties of a Message class
     * instance.
     */
    struct MessageImpl {
        bool isElectionMessage = false;
    };

}
