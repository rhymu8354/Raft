#pragma once

/**
 * @file Command.hpp
 *
 * This module declares the Raft::Command structure.
 *
 * © 2018-2020 by Richard Walters
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
