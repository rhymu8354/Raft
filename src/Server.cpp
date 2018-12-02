/**
 * @file Server.cpp
 *
 * This module contains the implementation of the Raft::Server class.
 *
 * © 2018 by Richard Walters
 */

#include <Raft/Server.hpp>
#include <Raft/TimeKeeper.hpp>
#include <SystemAbstractions/DiagnosticsSender.hpp>

namespace {

    /**
     * This contains the private properties of a Server class instance
     * that may live longer than the Server class instance itself.
     */
    struct ServerSharedProperties {
        // Properties

        /**
         * This is a helper object used to generate and publish
         * diagnostic messages.
         */
        SystemAbstractions::DiagnosticsSender diagnosticsSender;

        /**
         * This holds all configuration items for the server.
         */
        Raft::IServer::Configuration configuration;

        // Methods

        ServerSharedProperties()
            : diagnosticsSender("Raft::Server")
        {
        }
    };

}

namespace Raft {

    /**
     * This contains the private properties of a Server class instance
     * that don't live any longer than the Server class instance itself.
     */
    struct Server::Impl {
        /**
         * This holds any properties of the Server that might live longer
         * than the Server itself (e.g. due to being captured in
         * callbacks).
         */
        std::shared_ptr< ServerSharedProperties > shared = std::make_shared< ServerSharedProperties >();

        /**
         * This is the object used to track time for the server.
         */
        std::shared_ptr< TimeKeeper > timeKeeper;
    };

    Server::~Server() noexcept = default;
    Server::Server(Server&&) noexcept = default;
    Server& Server::operator=(Server&&) noexcept = default;

    Server::Server()
        : impl_(new Impl())
    {
    }

    SystemAbstractions::DiagnosticsSender::UnsubscribeDelegate Server::SubscribeToDiagnostics(
        SystemAbstractions::DiagnosticsSender::DiagnosticMessageDelegate delegate,
        size_t minLevel
    ) {
        return impl_->shared->diagnosticsSender.SubscribeToDiagnostics(delegate, minLevel);
    }

    void Server::SetTimeKeeper(std::shared_ptr< TimeKeeper > timeKeeper) {
        impl_->timeKeeper = timeKeeper;
    }

    bool Server::Configure(const Configuration& configuration) {
        impl_->shared->configuration = configuration;
        return true;
    }

    auto Server::GetConfiguration() const -> const Configuration& {
        return impl_->shared->configuration;
    }

}
