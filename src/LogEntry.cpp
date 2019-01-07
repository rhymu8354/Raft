/**
 * @file LogEntry.cpp
 *
 * This module contains the implementation of the Raft::LogEntry structure
 * methods.
 *
 * © 2018 by Richard Walters
 */

#include <Json/Value.hpp>
#include <map>
#include <Raft/LogEntry.hpp>

namespace {

    struct CommandFactories {
        // Properties

        /**
         * This holds all registered command factories, keyed by command type.
         */
        std::map< std::string, Raft::LogEntry::CommandFactory > factoriesByType;

        // Methods

        /**
         * This is the default constructor.
         */
        CommandFactories() {
            factoriesByType["SingleConfiguration"] = [](
                const Json::Value& commandAsJson
            ) {
                return std::make_shared< Raft::SingleConfigurationCommand >(commandAsJson);
            };
            factoriesByType["JointConfiguration"] = [](
                const Json::Value& commandAsJson
            ) {
                return std::make_shared< Raft::JointConfigurationCommand >(commandAsJson);
            };
        }
    } COMMAND_FACTORIES;

}

namespace Raft {

    SingleConfigurationCommand::SingleConfigurationCommand(const Json::Value& json) {
        const auto& instanceIds = json["configuration"]["instanceIds"];
        for (size_t i = 0; i < instanceIds.GetSize(); ++i) {
            (void)configuration.instanceIds.insert(instanceIds[i]);
        }
        const auto& oldInstanceIds = json["oldConfiguration"]["instanceIds"];
        for (size_t i = 0; i < oldInstanceIds.GetSize(); ++i) {
            (void)oldConfiguration.instanceIds.insert(oldInstanceIds[i]);
        }
    }

    std::string SingleConfigurationCommand::GetType() const {
        return "SingleConfiguration";
    }

    Json::Value SingleConfigurationCommand::Encode() const {
        auto instanceIdsArray = Json::Array({});
        for (const auto& instanceId: configuration.instanceIds) {
            instanceIdsArray.Add(instanceId);
        }
        auto oldInstanceIdsArray = Json::Array({});
        for (const auto& oldInstanceId: oldConfiguration.instanceIds) {
            oldInstanceIdsArray.Add(oldInstanceId);
        }
        return Json::Object({
            {"configuration", Json::Object({
                {"instanceIds", std::move(instanceIdsArray)},
            })},
            {"oldConfiguration", Json::Object({
                {"instanceIds", std::move(oldInstanceIdsArray)},
            })},
        });
    }

    JointConfigurationCommand::JointConfigurationCommand(const Json::Value& json) {
        const auto& oldInstanceIds = json["oldConfiguration"]["instanceIds"];
        for (size_t i = 0; i < oldInstanceIds.GetSize(); ++i) {
            (void)oldConfiguration.instanceIds.insert(oldInstanceIds[i]);
        }
        const auto& newInstanceIds = json["newConfiguration"]["instanceIds"];
        for (size_t i = 0; i < newInstanceIds.GetSize(); ++i) {
            (void)newConfiguration.instanceIds.insert(newInstanceIds[i]);
        }
    }

    std::string JointConfigurationCommand::GetType() const {
        return "JointConfiguration";
    }

    Json::Value JointConfigurationCommand::Encode() const {
        auto oldInstanceIdsArray = Json::Array({});
        for (const auto& instanceId: oldConfiguration.instanceIds) {
            oldInstanceIdsArray.Add(instanceId);
        }
        auto newInstanceIdsArray = Json::Array({});
        for (const auto& instanceId: newConfiguration.instanceIds) {
            newInstanceIdsArray.Add(instanceId);
        }
        return Json::Object({
            {"oldConfiguration", Json::Object({
                {"instanceIds", std::move(oldInstanceIdsArray)},
            })},
            {"newConfiguration", Json::Object({
                {"instanceIds", std::move(newInstanceIdsArray)},
            })},
        });
    }

    LogEntry::LogEntry(const std::string& serialization)
        : LogEntry(Json::Value::FromEncoding(serialization))
    {
    }

    LogEntry::LogEntry(const Json::Value& json) {
        term = json["term"];
        const std::string typeAsString = json["type"];
        const auto factory = COMMAND_FACTORIES.factoriesByType.find(typeAsString);
        if (factory != COMMAND_FACTORIES.factoriesByType.end()) {
            command = factory->second(json["command"]);
        }
    }

    LogEntry::operator std::string() const {
        Json::Value json = *this;
        return json.ToEncoding();
    }

    LogEntry::operator Json::Value() const {
        auto json = Json::Object({});
        json["term"] = term;
        if (command != nullptr) {
            json["type"] = command->GetType();
            json["command"] = command->Encode();
        }
        return json;
    }

    bool LogEntry::operator==(const LogEntry& other) const {
        return (Json::Value)*this == (Json::Value)other;
    }

    bool LogEntry::operator!=(const LogEntry& other) const {
        return (Json::Value)*this != (Json::Value)other;
    }

    void LogEntry::RegisterCommandType(
        const std::string& type,
        CommandFactory factory
    ) {
        COMMAND_FACTORIES.factoriesByType[type] = factory;
    }

}
