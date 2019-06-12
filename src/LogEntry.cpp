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
#include <Serialization/SerializedInteger.hpp>
#include <Serialization/SerializedString.hpp>

namespace {

    constexpr int CURRENT_SERIALIZATION_VERSION = 1;

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

    LogEntry::LogEntry(const Json::Value& json) {
        term = json["term"];
        const std::string typeAsString = json["type"];
        const auto factory = COMMAND_FACTORIES.factoriesByType.find(typeAsString);
        if (factory != COMMAND_FACTORIES.factoriesByType.end()) {
            command = factory->second(json["command"]);
        }
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
        if (term != other.term) {
            return false;
        }
        if (command == nullptr) {
            return (other.command == nullptr);
        } else if (other.command == nullptr) {
            return false;
        }
        if (command->GetType() != other.command->GetType()) {
            return false;
        }
        return (command->Encode() == other.command->Encode());
    }

    bool LogEntry::operator!=(const LogEntry& other) const {
        return !(*this == other);
    }

    void LogEntry::RegisterCommandType(
        const std::string& type,
        CommandFactory factory
    ) {
        COMMAND_FACTORIES.factoriesByType[type] = factory;
    }

    bool LogEntry::Serialize(
        SystemAbstractions::IFile* file,
        unsigned int serializationVersion
    ) const {
        if (serializationVersion > CURRENT_SERIALIZATION_VERSION) {
            return false;
        } else if (serializationVersion == 0) {
            serializationVersion = CURRENT_SERIALIZATION_VERSION;
        }
        Serialization::SerializedInteger intField(serializationVersion);
        if (!intField.Serialize(file)) {
            return false;
        }
        intField = term;
        if (!intField.Serialize(file)) {
            return false;
        }
        Serialization::SerializedString stringField(
            (command == nullptr)
            ? ""
            : command->GetType()
        );
        if (!stringField.Serialize(file)) {
            return false;
        }
        if (command != nullptr) {
            stringField = command->Encode().ToEncoding();
            if (!stringField.Serialize(file)) {
                return false;
            }
        }
        return true;
    }

    bool LogEntry::Deserialize(SystemAbstractions::IFile* file) {
        Serialization::SerializedInteger intField;
        if (!intField.Deserialize(file)) {
            return false;
        }
        const auto version = (int)intField;
        if (version > CURRENT_SERIALIZATION_VERSION) {
            return false;
        }
        if (!intField.Deserialize(file)) {
            return false;
        }
        term = (int)intField;
        Serialization::SerializedString stringField;
        if (!stringField.Deserialize(file)) {
            return false;
        }
        const std::string typeAsString = stringField;
        if (typeAsString.empty()) {
            command = nullptr;
        } else {
            if (!stringField.Deserialize(file)) {
                return false;
            }
            const auto encodedCommand = Json::Value::FromEncoding(stringField);
            const auto factory = COMMAND_FACTORIES.factoriesByType.find(typeAsString);
            if (factory != COMMAND_FACTORIES.factoriesByType.end()) {
                command = factory->second(encodedCommand);
            }
        }
        return true;
    }

    std::string LogEntry::Render() const {
        return "";
    }

    bool LogEntry::Parse(std::string rendering) {
        return false;
    }

    bool LogEntry::IsEqualTo(const ISerializedObject* other) const {
        return false;
    }


}
