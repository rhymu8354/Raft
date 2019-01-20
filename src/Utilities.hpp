#ifndef RAFT_UTILITIES_HPP
#define RAFT_UTILITIES_HPP

/**
 * @file Utilities.hpp
 *
 * This module contains the declaration of free functions used by other parts
 * of the library implementation.
 *
 * © 2019 by Richard Walters
 */

#include "ConfigCommittedAnnouncement.hpp"
#include "LeadershipAnnouncement.hpp"
#include "MessageToBeSent.hpp"
#include "ServerImpl.hpp"

#include <set>
#include <string>
#include <sstream>
#include <queue>
#include <Raft/IServer.hpp>
#include <Raft/ClusterConfiguration.hpp>

namespace Raft {

    /**
     * This is a support function for Google Test to print out
     * values of the Raft::IServer::ElectionState type.
     *
     * @param[in] electionState
     *     This is the election state value to print.
     *
     * @param[in] os
     *     This points to the stream to which to print the
     *     election state value.
     */
    void PrintTo(
        const Raft::IServer::ElectionState& electionState,
        std::ostream* os
    );

    /**
     * Return a human-readable string representation of the given server
     * election state.
     *
     * @param[in] electionState
     *     This is the election state to turn into a string.
     *
     * @return
     *     A human-readable string representation of the given server
     *     election state is returned.
     */
    std::string ElectionStateToString(Raft::Server::ElectionState electionState);

    /**
     * This is the template for a function which builds and returns a
     * human-readable string representation of a set of elements.
     *
     * @param[in] s
     *     This is the set of elements to format as a string.
     *
     * @return
     *     A human-readable representation of the given set is returned.
     */
    template< typename T > std::string FormatSet(const std::set< T >& s) {
        std::ostringstream builder;
        builder << '{';
        bool first = true;
        for (const auto& element: s) {
            if (!first) {
                builder << ", ";
            }
            first = false;
            builder << element;
        }
        builder << '}';
        return builder.str();
    }

    /**
     * This function sends the given messages to other servers, using the given
     * delegate.
     *
     * @param[in] sendMessageDelegate
     *     This is the delegate to use to send messages to other servers.
     *
     * @param[in,out] messagesToBeSent
     *     This holds the messages to be sent, and is consumed by the function.
     */
    void SendMessages(
        Raft::IServer::SendMessageDelegate sendMessageDelegate,
        std::queue< MessageToBeSent >&& messagesToBeSent
    );

    /**
     * This function sends the given leadership announcements, using the given
     * delegate.
     *
     * @param[in] leadershipChangeDelegate
     *     This is the delegate to use to send leadership announcements.
     *
     * @param[in,out] leadershipAnnouncementsToBeSent
     *     This holds the leadership announcements to be sent, and is consumed
     *     by the function.
     */
    void SendLeadershipAnnouncements(
        Raft::IServer::LeadershipChangeDelegate leadershipChangeDelegate,
        std::queue< LeadershipAnnouncement >&& leadershipAnnouncementsToBeSent
    );

    /**
     * Send the given election state change announcements, using the given
     * delegate.
     *
     * @param[in] electionStateChangeDelegate
     *     This is the delegate to use to send election state change
     *     announcements.
     *
     * @param[in,out] electionStateChangeAnnouncementsToBeSent
     *     This holds the election state change announcements to be sent,
     *     and is consumed by the function.
     */
    void SendElectionStateChangeAnnouncements(
        Raft::IServer::ElectionStateChangeDelegate electionStateChangeDelegate,
        std::queue< ElectionStateChangeAnnouncement >&& electionStateChangeAnnouncementsToBeSent
    );

    /**
     * This function sends the given configuration applied announcements, using
     * the given delegate.
     *
     * @param[in] applyConfigurationDelegate
     *     This is the delegate to use to send configuration applied
     *     announcements.
     *
     * @param[in,out] configAppliedAnnouncementsToBeSent
     *     This holds the configuration applied announcements to be sent, and
     *     is consumed by the function.
     */
    void SendConfigAppliedAnnouncements(
        Raft::IServer::ApplyConfigurationDelegate applyConfigurationDelegate,
        std::queue< Raft::ClusterConfiguration >&& configAppliedAnnouncementsToBeSent
    );

    /**
     * This function sends the given configuration committed announcements,
     * using the given delegate.
     *
     * @param[in] commitConfigurationDelegate
     *     This is the delegate to use to send configuration committed
     *     announcements.
     *
     * @param[in,out] configCommittedAnnouncementsToBeSent
     *     This holds the configuration committed announcements to be sent,
     *     and is consumed by the function.
     */
    void SendConfigCommittedAnnouncements(
        Raft::IServer::CommitConfigurationDelegate commitConfigurationDelegate,
        std::queue< ConfigCommittedAnnouncement >&& configCommittedAnnouncementsToBeSent
    );

}

#endif /* RAFT_UTILITIES_HPP */
