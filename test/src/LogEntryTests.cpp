/**
 * @file LogEntryTests.cpp
 *
 * This module contains the unit tests of the
 * Raft::LogEntry class.
 *
 * © 2018-2020 by Richard Walters
 */

#include <gtest/gtest.h>
#include <Json/Value.hpp>
#include <Raft/LogEntry.hpp>

TEST(LogEntryTests, Encode_Single_Configuration_Command) {
    // Arrange
    auto command = std::make_shared< Raft::SingleConfigurationCommand >();
    command->oldConfiguration.instanceIds = {5, 42, 85, 13531, 8354};
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
                {"oldConfiguration", Json::Object({
                    {"instanceIds", Json::Array({5, 42, 85, 8354, 13531})},
                })},
                {"configuration", Json::Object({
                    {"instanceIds", Json::Array({42, 85, 8354, 13531})},
                })},
            })},
        }),
        entry
    );
}

TEST(LogEntryTests, Decode_Single_Configuration_Command) {
    // Arrange
    auto encodedEntry = Json::Object({
        {"type", "SingleConfiguration"},
        {"term", 9},
        {"command", Json::Object({
            {"oldConfiguration", Json::Object({
                {"instanceIds", Json::Array({5, 42, 85, 8354, 13531})},
            })},
            {"configuration", Json::Object({
                {"instanceIds", Json::Array({42, 85, 8354, 13531})},
            })},
        })},
    });

    // Act
    const auto entry = Raft::LogEntry(encodedEntry);

    // Assert
    EXPECT_EQ(9, entry.term);
    EXPECT_EQ("SingleConfiguration", entry.command->GetType());
    const auto command = std::static_pointer_cast< Raft::SingleConfigurationCommand >(entry.command);
    EXPECT_EQ(
        std::set< int >({42, 85, 13531, 8354}),
        command->configuration.instanceIds
    );
    EXPECT_EQ(
        std::set< int >({5, 42, 85, 13531, 8354}),
        command->oldConfiguration.instanceIds
    );
}

TEST(LogEntryTests, Encode_Joint_Configuration_Command) {
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
        entry
    );
}

TEST(LogEntryTests, Decode_Joint_Configuration_Command) {
    // Arrange
    auto encodedEntry = Json::Object({
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
    const auto entry = Raft::LogEntry(encodedEntry);

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

TEST(LogEntryTests, To_Json_With_Command) {
    // Arrange
    auto command = std::make_shared< Raft::SingleConfigurationCommand >();
    command->oldConfiguration.instanceIds = {5, 42, 85, 13531, 8354};
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
                {"oldConfiguration", Json::Object({
                    {"instanceIds", Json::Array({5, 42, 85, 8354, 13531})},
                })},
                {"configuration", Json::Object({
                    {"instanceIds", Json::Array({42, 85, 8354, 13531})},
                })},
            })},
        }),
        entry
    );
}

TEST(LogEntryTests, From_Json_With_Command) {
    // Arrange
    auto entryAsJson = Json::Object({
        {"type", "SingleConfiguration"},
        {"term", 9},
        {"command", Json::Object({
            {"oldConfiguration", Json::Object({
                {"instanceIds", Json::Array({5, 42, 85, 8354, 13531})},
            })},
            {"configuration", Json::Object({
                {"instanceIds", Json::Array({42, 85, 8354, 13531})},
            })},
        })},
    });

    // Act
    const Raft::LogEntry entry(entryAsJson);

    // Assert
    EXPECT_EQ(9, entry.term);
    ASSERT_FALSE(entry.command == nullptr);
    EXPECT_EQ("SingleConfiguration", entry.command->GetType());
    const auto command = std::static_pointer_cast< Raft::SingleConfigurationCommand >(entry.command);
    EXPECT_EQ(
        std::set< int >({42, 85, 13531, 8354}),
        command->configuration.instanceIds
    );
    EXPECT_EQ(
        std::set< int >({5, 42, 85, 13531, 8354}),
        command->oldConfiguration.instanceIds
    );
}

TEST(LogEntryTests, To_Json_Without_Command) {
    // Arrange
    Raft::LogEntry entry;
    entry.term = 9;

    // Act
    EXPECT_EQ(
        Json::Object({
            {"term", 9},
        }),
        entry
    );
}

TEST(LogEntryTests, From_Json_Without_Command) {
    // Arrange
    auto entryAsJson = Json::Object({
        {"term", 9},
    });

    // Act
    const Raft::LogEntry entry(entryAsJson);

    // Assert
    EXPECT_EQ(9, entry.term);
    EXPECT_TRUE(entry.command == nullptr);
}

TEST(LogEntryTests, Compare_Equal) {
    // Arrange
    std::vector< Raft::LogEntry > examples{
        Json::Object({
            {"type", "SingleConfiguration"},
            {"term", 9},
            {"command", Json::Object({
                {"oldConfiguration", Json::Object({
                    {"instanceIds", Json::Array({5, 42, 85, 8354, 13531})},
                })},
                {"configuration", Json::Object({
                    {"instanceIds", Json::Array({42, 85, 8354, 13531})},
                })},
            })},
        }),
        Json::Object({
            {"type", "SingleConfiguration"},
            {"term", 8},
            {"command", Json::Object({
                {"oldConfiguration", Json::Object({
                    {"instanceIds", Json::Array({5, 42, 85, 8354, 13531})},
                })},
                {"configuration", Json::Object({
                    {"instanceIds", Json::Array({42, 85, 8354, 13531})},
                })},
            })},
        }),
        Json::Object({
            {"type", "SingleConfiguration"},
            {"term", 9},
            {"command", Json::Object({
                {"oldConfiguration", Json::Object({
                    {"instanceIds", Json::Array({5, 42, 85, 8354, 13531})},
                })},
                {"configuration", Json::Object({
                    {"instanceIds", Json::Array({5, 85, 8354, 13531})},
                })},
            })},
        }),
        Json::Object({
            {"term", 8},
        }),
        Json::Object({
            {"term", 9},
        }),
    };

    // Act
    const size_t numExamples = examples.size();
    for (size_t i = 0; i < numExamples; ++i) {
        for (size_t j = 0; j < numExamples; ++j) {
            if (i == j) {
                EXPECT_EQ(examples[i], examples[j]);
            } else {
                EXPECT_NE(examples[i], examples[j]);
            }
        }
    }
}

TEST(LogEntryTests, Custom_Command) {
    // Arrange
    struct PogChamp : public Raft::Command {
        int payload = 0;
        virtual std::string GetType() const override { return "PogChamp"; }
        virtual Json::Value Encode() const override {
            return Json::Object({
                {"payload", payload},
            });
        }
    };
    const auto pogChampFactory = [](
        const Json::Value& commandAsJson
    ) {
        const auto pogChamp = std::make_shared< PogChamp >();
        pogChamp->payload = commandAsJson["payload"];
        return pogChamp;
    };

    // Act
    Raft::LogEntry::RegisterCommandType(
        "PogChamp",
        pogChampFactory
    );
    const auto pogChamp = std::make_shared< PogChamp >();
    pogChamp->payload = 42;
    Raft::LogEntry pogChampEntry;
    pogChampEntry.term = 8;
    pogChampEntry.command = pogChamp;
    const Json::Value serializedPogChamp = pogChampEntry;
    EXPECT_EQ(
        Json::Object({
            {"term", 8},
            {"type", "PogChamp"},
            {"command", Json::Object({
                {"payload", 42},
            })},
        }),
        serializedPogChamp
    );
    pogChampEntry = Raft::LogEntry(serializedPogChamp);

    // Assert
    EXPECT_EQ(8, pogChampEntry.term);
    ASSERT_FALSE(pogChampEntry.command == nullptr);
    EXPECT_EQ("PogChamp", pogChampEntry.command->GetType());
    const auto command = std::static_pointer_cast< PogChamp >(pogChampEntry.command);
    EXPECT_EQ(42, command->payload);
}
