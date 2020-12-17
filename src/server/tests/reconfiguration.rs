// TODO:
// * Include the cluster configuration in snapshot.
// * A request to change configuration to one in which peers are added should
//   cause the leader to hold the configuration change in a "pending" state, add
//   the new peers as "non-voting" members (which do not count towards majority
//   needed to commit) to its peer list, and begin replicating the log to them.
//   A copy of the log index of the leader at the time of the configuration
//   change request is held onto in order to determine when the "non-voting"
//   members have caught up.
// * If a configuration change is pending and all "non-voting" members have
//   matched up to predetermined "catch up" point, leader should append log
//   entry for joint configuration.
// * If a configuration change is requested and no new peers are added, leader
//   should append log entry for joint configuration.
// * A second configuration change request made while a previous one is still
//   held in the "pending" state should be treated as if the previous one was
//   never made; the "catch up" point is recalculated, "non-voting" members are
//   reconsidered, etc.
// * A request to change the configuration to the current configuration should
//   be ignored.
// * A request to change the configuration while the cluster is in joint
//   configuration should be ignored.
// * Server should use latest configuration appended to log, even if it hasn't
//   yet been committed.
// * When the last joint configuration log entry is committed, the leader should
//   append log entry for the new configuration.
// * A newly elected leader should inspect its log, from newest backwards to its
//   snapshot, to see what configuration is in effect.  If it differs from the
//   configuration provided by the user, a "configuration change" event should
//   be generated.
// * A server whose own ID is not in the cluster configuration should consider
//   itself a "non-voting" member; it should not respond to vote requests and
//   should not start elections.
// * A "non-voting" member should transition to "voting" member when its own ID
//   becomes present in the latest cluster configuration; it should start its
//   election timer and begin responding to vote requests.
// * A "voting" member should transition to "non-voting" member when its own ID
//   is no longer in the latest cluster configuration; it should cancel its
//   election timer and stop responding to vote requests.
// * Once the current configuration is committed, if the leader was a
//   "non-voting" member, it should step down by no longer appending entries.
//   (At this point we could let it delegate leadership explicitly, or simply
//   let one of the other servers start a new election once its election timer
//   expires.)
