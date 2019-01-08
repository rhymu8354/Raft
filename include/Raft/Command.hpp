#ifndef RAFT_COMMAND_HPP
#define RAFT_COMMAND_HPP

/**
 * @file Command.hpp
 *
 * This module declares the Raft::Command structure.
 *
 * © 2018 by Richard Walters
 */

#include <Json/Value.hpp>
#include <string>

namespace Raft {

    class Command {
    public:
        virtual std::string GetType() const = 0;
        virtual Json::Value Encode() const = 0;
    };

}

#endif /* RAFT_COMMAND_HPP */
