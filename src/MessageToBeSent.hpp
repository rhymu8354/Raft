#ifndef RAFT_MESSAGE_TO_BE_SENT_HPP
#define RAFT_MESSAGE_TO_BE_SENT_HPP

/**
 * @file MessageToBeSent.hpp
 *
 * This module contains the declaration of the Raft::MessageToBeSent structure.
 *
 * © 2019 by Richard Walters
 */

#include <string>

namespace Raft {

    /**
     * This holds information used to store a message to be sent, and later to
     * send the message.
     */
    struct MessageToBeSent {
        /**
         * This is the message to be sent.
         */
        std::string message;

        /**
         * This is the unique identifier of the server to which to send the
         * message.
         */
        int receiverInstanceNumber = 0;
    };

}

#endif /* RAFT_MESSAGE_TO_BE_SENT_HPP */
