#ifndef RAFT_LOG_ENTRY_HPP
#define RAFT_LOG_ENTRY_HPP

/**
 * @file LogEntry.hpp
 *
 * This module declares the Raft::LogEntry implementation.
 *
 * © 2018 by Richard Walters
 */

#include "ClusterConfiguration.hpp"

#include <Json/Value.hpp>
#include <memory>
#include <string>

namespace Raft {

    struct Command {
        virtual std::string GetType() const = 0;
        virtual Json::Value Encode() const = 0;
    };

    struct SingleConfigurationCommand
        : public Command
    {
        // Properties

        ClusterConfiguration configuration;
        ClusterConfiguration oldConfiguration;

        // Methods

        SingleConfigurationCommand(const Json::Value& json = nullptr);

        // Command

        virtual std::string GetType() const override;
        virtual Json::Value Encode() const override;
    };

    struct JointConfigurationCommand
        : public Command
    {
        // Properties

        ClusterConfiguration oldConfiguration;
        ClusterConfiguration newConfiguration;

        // Methods

        JointConfigurationCommand(const Json::Value& json = nullptr);

        // Command

        virtual std::string GetType() const override;
        virtual Json::Value Encode() const override;
    };

    /**
     * This is the base class for log entries of a server which uses the Raft
     * Consensus Algorithm.  It contains all the properties and methods which
     * directly relate to the algorithm.  It is meant to be subclassed in order
     * to hold actual concrete server state.
     */
    struct LogEntry {
        // Properties

        /**
         * This is the term when the entry was received by the leader.
         */
        int term = 0;

        /**
         * This represents the change to be made to the server state when
         * this log entry is applied.
         */
        std::shared_ptr< Command > command;

        // Methods

        /**
         * This is the constructor of the class.
         *
         * @param[in] serialization
         *     If not empty, this is the serialized form of the log entry, used
         *     to initialize the type and properties of the log entry.
         */
        LogEntry(const std::string& serialization = "");

        /**
         * This is the constructor of the class.
         *
         * @param[in] serialization
         *     If not empty, this is the serialized form of the log entry, used
         *     to initialize the type and properties of the log entry.
         */
        LogEntry(const Json::Value& json);

        /**
         * This method returns a string which can be used to construct a new
         * log entry with the exact same contents as this log entry.
         *
         * @return
         *     A string which can be used to construct a new log entry with the
         *     exact same contents as this log entry is returned.
         */
        operator std::string() const;

        /**
         * This method returns a JSON value which can be used to construct a
         * new log entry with the exact same contents as this log entry.
         *
         * @return
         *     A JSON value which can be used to construct a new log entry
         *     with the exact same contents as this log entry is returned.
         */
        operator Json::Value() const;

        /**
         * Compare this log entry with the given other log entry.
         *
         * @param[in] other
         *     This is the other log entry with which to compare this
         *     log entry.
         *
         * @return
         *     An indication of whether or not the two log entries are equal
         *     is returned.
         */
        bool operator==(const LogEntry& other) const;

        /**
         * Compare this log entry with the given other log entry.
         *
         * @param[in] other
         *     This is the other log entry with which to compare this
         *     log entry.
         *
         * @return
         *     An indication of whether or not the two log entries are
         *     not equal is returned.
         */
        bool operator!=(const LogEntry& other) const;
    };

}

#endif /* RAFT_LOG_ENTRY_HPP */
