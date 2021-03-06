#pragma once

/**
 * @file LogEntry.hpp
 *
 * This module declares the Raft::LogEntry structure.
 *
 * © 2018-2020 by Richard Walters
 */

#include "ClusterConfiguration.hpp"
#include "Command.hpp"

#include <functional>
#include <Json/Value.hpp>
#include <memory>
#include <string>
#include <Serialization/ISerializedObject.hpp>

namespace Raft {

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
    struct LogEntry
        : public Serialization::ISerializedObject
    {
        // Types

        /**
         * Define the type of function responsible for creating a command
         * of a specific type based on its serialized form.
         *
         * @param[in] commandAsJson
         *     This is the serialized form of the command to create.
         *
         * @return
         *     The newly created command is returned.
         */
        using CommandFactory = std::function<
            std::shared_ptr< Command >(
                const Json::Value& commandAsJson
            )
        >;

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
         * Construct a default, empty LogEntry.
         */
        LogEntry() = default;

        /**
         * Construct a LogEntry from its encoded JSON form.
         *
         * @param[in] json
         *     If not empty, this is the encoded form of the log entry, used
         *     to initialize the type and properties of the log entry.
         */
        LogEntry(const Json::Value& json);

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

        /**
         * Register the given command type in order to enable the creation
         * of commands of the type when deserializing log entries.
         *
         * @param[in] type
         *     This is the string identifier for the command type.
         *
         * @param[in] factory
         *     This is the function to call to create a command of this type
         *     from its serialized form.
         */
        static void RegisterCommandType(
            const std::string& type,
            CommandFactory factory
        );

        // ISerializedObject
    public:
        virtual bool Serialize(
            SystemAbstractions::IFile* file,
            unsigned int serializationVersion = 0
        ) const override;
        virtual bool Deserialize(SystemAbstractions::IFile* file) override;
        virtual std::string Render() const override;
        virtual bool Parse(std::string rendering) override;
        virtual bool IsEqualTo(const ISerializedObject* other) const override;
    };

}
