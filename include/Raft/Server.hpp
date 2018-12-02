#ifndef RAFT_SERVER_HPP
#define RAFT_SERVER_HPP

/**
 * @file Server.hpp
 *
 * This module declares the Raft::Server implementation.
 *
 * © 2018 by Richard Walters
 */

#include <memory>
#include <Raft/IServer.hpp>
#include <Raft/TimeKeeper.hpp>
#include <stddef.h>
#include <SystemAbstractions/DiagnosticsSender.hpp>

namespace Raft {

    /**
     * This class represents one member of the server cluster.
     */
    class Server
        : public IServer
    {
        // Lifecycle Methods
    public:
        ~Server() noexcept;
        Server(const Server&) = delete;
        Server(Server&&) noexcept;
        Server& operator=(const Server&) = delete;
        Server& operator=(Server&&) noexcept;

        // Public Methods
    public:
        /**
         * This is the constructor of the class.
         */
        Server();

        /**
         * This method forms a new subscription to diagnostic
         * messages published by the class.
         *
         * @param[in] delegate
         *     This is the function to call to deliver messages
         *     to the subscriber.
         *
         * @param[in] minLevel
         *     This is the minimum level of message that this subscriber
         *     desires to receive.
         *
         * @return
         *     A function is returned which may be called
         *     to terminate the subscription.
         */
        SystemAbstractions::DiagnosticsSender::UnsubscribeDelegate SubscribeToDiagnostics(
            SystemAbstractions::DiagnosticsSender::DiagnosticMessageDelegate delegate,
            size_t minLevel = 0
        );

        /**
         * This method is called to provide the object used to track time
         * for the server.
         *
         * @param[in] timeKeeper
         *     This is the object used to track time for the server.
         */
        void SetTimeKeeper(std::shared_ptr< TimeKeeper > timeKeeper);

        /**
         * This method returns the server's current configuration.
         *
         * @return
         *     The server's current configuration is returned.
         */
        const Configuration& GetConfiguration() const;

        // IServer
    public:
        virtual bool Configure(const Configuration& configuration) override;

        // Private properties
    private:
        /**
         * This is the type of structure that contains the private
         * properties of the instance.  It is defined in the implementation
         * and declared here to ensure that it is scoped inside the class.
         */
        struct Impl;

        /**
         * This contains the private properties of the instance.
         */
        std::unique_ptr< Impl > impl_;
    };

}

#endif /* RAFT_SERVER_HPP */
