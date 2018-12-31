/**
 * @file LogEntry.cpp
 *
 * This module contains the implementation of the Raft::LogEntry structure
 * methods.
 *
 * © 2018 by Richard Walters
 */

#include <Json/Value.hpp>
#include <Raft/LogEntry.hpp>

namespace Raft {

    SingleConfigurationCommand::SingleConfigurationCommand(const Json::Value& json) {
        const auto& instanceIds = json["configuration"]["instanceIds"];
        for (size_t i = 0; i < instanceIds.GetSize(); ++i) {
            (void)configuration.instanceIds.insert(instanceIds[i]);
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
        return Json::Object({
            {"configuration", Json::Object({
                {"instanceIds", std::move(instanceIdsArray)},
            })},
        });
    }

    ProvisionalConfigurationCommand::ProvisionalConfigurationCommand(const Json::Value& json) {
        const auto& oldInstanceIds = json["oldConfiguration"]["instanceIds"];
        for (size_t i = 0; i < oldInstanceIds.GetSize(); ++i) {
            (void)oldConfiguration.instanceIds.insert(oldInstanceIds[i]);
        }
        const auto& newInstanceIds = json["newConfiguration"]["instanceIds"];
        for (size_t i = 0; i < newInstanceIds.GetSize(); ++i) {
            (void)newConfiguration.instanceIds.insert(newInstanceIds[i]);
        }
    }

    std::string ProvisionalConfigurationCommand::GetType() const {
        return "ProvisionalConfiguration";
    }

    Json::Value ProvisionalConfigurationCommand::Encode() const {
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

    LogEntry::LogEntry(const std::string& serialization) {
        const auto json = Json::Value::FromEncoding(serialization);
        term = json["term"];
        const std::string typeAsString = json["type"];
        if (typeAsString == "SingleConfiguration") {
            const auto singleConfigurationCommand = std::make_shared< SingleConfigurationCommand >(
                json["command"]
            );
            command = std::move(singleConfigurationCommand);
        } else if (typeAsString == "ProvisionalConfiguration") {
            const auto provisionalConfigurationCommand = std::make_shared< ProvisionalConfigurationCommand >(
                json["command"]
            );
            command = std::move(provisionalConfigurationCommand);
        } else if (typeAsString == "JointConfiguration") {
            const auto jointConfigurationCommand = std::make_shared< JointConfigurationCommand >(
                json["command"]
            );
            command = std::move(jointConfigurationCommand);
        }
    }

    std::string LogEntry::Serialize() const {
        auto json = Json::Object({});
        json["type"] = command->GetType();
        json["term"] = term;
        json["command"] = command->Encode();
        return json.ToEncoding();
    }

}
