/**
 * @file LogEntryTests.cpp
 *
 * This module contains the unit tests of the
 * Raft::LogEntry class.
 *
 * © 2018 by Richard Walters
 */

#include <gtest/gtest.h>
#include <Json/Value.hpp>
#include <Raft/LogEntry.hpp>

TEST(LogEntryTests, SerializeSingleConfigurationCommand) {
    // Arrange
    auto command = std::make_shared< Raft::SingleConfigurationCommand >();
    command->configuration.instanceIds = {42, 85, 13531, 8354};
    Raft::LogEntry entry;
    entry.term = 9;
    entry.command = std::move(command);

    // Act
    EXPECT_EQ(
        Json::Object({
            {"type", "SingleConfiguration"},
            {"term", 9},
            {"command", Json::Object({
                {"configuration", Json::Object({
                    {"instanceIds", Json::Array({42, 85, 8354, 13531})},
                })},
            })},
        }),
        Json::Value::FromEncoding(entry.Serialize())
    );
}

TEST(LogEntryTests, DeserializeSingleConfigurationCommand) {
    // Arrange
    auto serializedEntry = Json::Object({
        {"type", "SingleConfiguration"},
        {"term", 9},
        {"command", Json::Object({
            {"configuration", Json::Object({
                {"instanceIds", Json::Array({42, 85, 8354, 13531})},
            })},
        })},
    });

    // Act
    const auto entry = Raft::LogEntry(serializedEntry.ToEncoding());

    // Assert
    EXPECT_EQ(9, entry.term);
    EXPECT_EQ("SingleConfiguration", entry.command->GetType());
    const auto command = std::static_pointer_cast< Raft::SingleConfigurationCommand >(entry.command);
    EXPECT_EQ(
        std::set< int >({42, 85, 13531, 8354}),
        command->configuration.instanceIds
    );
}

TEST(LogEntryTests, SerializeProvisionalConfigurationCommand) {
    // Arrange
    auto command = std::make_shared< Raft::ProvisionalConfigurationCommand >();
    command->oldConfiguration.instanceIds = {42, 85, 13531, 8354};
    command->newConfiguration.instanceIds = {10, 42, 85, 13531, 8354};
    Raft::LogEntry entry;
    entry.term = 9;
    entry.command = std::move(command);

    // Act
    EXPECT_EQ(
        Json::Object({
            {"type", "ProvisionalConfiguration"},
            {"term", 9},
            {"command", Json::Object({
                {"oldConfiguration", Json::Object({
                    {"instanceIds", Json::Array({42, 85, 8354, 13531})},
                })},
                {"newConfiguration", Json::Object({
                    {"instanceIds", Json::Array({10, 42, 85, 8354, 13531})},
                })},
            })},
        }),
        Json::Value::FromEncoding(entry.Serialize())
    );
}

TEST(LogEntryTests, DeserializeProvisionalConfigurationCommand) {
    // Arrange
    auto serializedEntry = Json::Object({
        {"type", "ProvisionalConfiguration"},
        {"term", 9},
        {"command", Json::Object({
            {"oldConfiguration", Json::Object({
                {"instanceIds", Json::Array({42, 85, 8354, 13531})},
            })},
            {"newConfiguration", Json::Object({
                {"instanceIds", Json::Array({10, 42, 85, 8354, 13531})},
            })},
        })},
    });

    // Act
    const auto entry = Raft::LogEntry(serializedEntry.ToEncoding());

    // Assert
    EXPECT_EQ(9, entry.term);
    EXPECT_EQ("ProvisionalConfiguration", entry.command->GetType());
    const auto command = std::static_pointer_cast< Raft::ProvisionalConfigurationCommand >(entry.command);
    EXPECT_EQ(
        std::set< int >({42, 85, 13531, 8354}),
        command->oldConfiguration.instanceIds
    );
    EXPECT_EQ(
        std::set< int >({10, 42, 85, 13531, 8354}),
        command->newConfiguration.instanceIds
    );
}

TEST(LogEntryTests, SerializeJointConfigurationCommand) {
    // Arrange
    auto command = std::make_shared< Raft::JointConfigurationCommand >();
    command->oldConfiguration.instanceIds = {42, 85, 13531, 8354};
    command->newConfiguration.instanceIds = {10, 42, 85, 13531, 8354};
    Raft::LogEntry entry;
    entry.term = 9;
    entry.command = std::move(command);

    // Act
    EXPECT_EQ(
        Json::Object({
            {"type", "JointConfiguration"},
            {"term", 9},
            {"command", Json::Object({
                {"oldConfiguration", Json::Object({
                    {"instanceIds", Json::Array({42, 85, 8354, 13531})},
                })},
                {"newConfiguration", Json::Object({
                    {"instanceIds", Json::Array({10, 42, 85, 8354, 13531})},
                })},
            })},
        }),
        Json::Value::FromEncoding(entry.Serialize())
    );
}

TEST(LogEntryTests, DeserializeJointConfigurationCommand) {
    // Arrange
    auto serializedEntry = Json::Object({
        {"type", "JointConfiguration"},
        {"term", 9},
        {"command", Json::Object({
            {"oldConfiguration", Json::Object({
                {"instanceIds", Json::Array({42, 85, 8354, 13531})},
            })},
            {"newConfiguration", Json::Object({
                {"instanceIds", Json::Array({10, 42, 85, 8354, 13531})},
            })},
        })},
    });

    // Act
    const auto entry = Raft::LogEntry(serializedEntry.ToEncoding());

    // Assert
    EXPECT_EQ(9, entry.term);
    EXPECT_EQ("JointConfiguration", entry.command->GetType());
    const auto command = std::static_pointer_cast< Raft::JointConfigurationCommand >(entry.command);
    EXPECT_EQ(
        std::set< int >({42, 85, 13531, 8354}),
        command->oldConfiguration.instanceIds
    );
    EXPECT_EQ(
        std::set< int >({10, 42, 85, 13531, 8354}),
        command->newConfiguration.instanceIds
    );
}