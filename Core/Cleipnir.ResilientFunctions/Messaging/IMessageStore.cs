using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public interface IMessageStore
{
    Task Initialize();

    /// <summary>
    /// Appends the messages to their target flows. Each message row is written with the target flow's
    /// current owner, or the message's publisher replica when the target is not executing; delivery is
    /// push-based via the MessageWatchdog, which also restart-claims not-live targets holding undelivered
    /// messages. Positions are assigned by the store in caller order.
    /// </summary>
    Task AppendMessages(IReadOnlyList<SerializedMessageWithReplicaId> messages);

    /// <summary>
    /// Deletes the messages at the given positions regardless of which flow they belong to. Positions are
    /// globally unique (identity values), so no <see cref="StoredId"/> is needed - allowing handled messages
    /// across many flows to be removed in a single query.
    /// </summary>
    Task DeleteMessages(IReadOnlyList<long> positions);

    Task Truncate(StoredId storedId);

    Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId);
    Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, IReadOnlyList<long> skipPositions);
    Task<Dictionary<StoredId, List<StoredMessage>>> GetMessages(IEnumerable<StoredId> storedIds);

    /// <summary>
    /// Returns the undelivered messages whose replica equals the provided replica, each carrying its target flow
    /// in <see cref="StoredMessage.StoredId"/> and ordered by position within that flow.
    /// Messages at any of the <paramref name="ignorePositions"/> are excluded - the MessageWatchdog passes the
    /// positions it has already pushed so they are not re-delivered on subsequent ticks.
    /// Used by the MessageWatchdog to push messages to live flows owned by this replica.
    /// </summary>
    Task<List<StoredMessage>> GetMessagesForReplica(ReplicaId replicaId, IReadOnlyList<long> ignorePositions);

    /// <summary>
    /// Returns the (flow, position) identifiers of the undelivered messages owned by a replica that is no
    /// longer alive (its replica is not contained in <paramref name="liveReplicas"/>).
    /// Used to detect messages stranded by crashed replicas so they can be re-assigned to a live replica via <see cref="SetReplica"/>.
    /// </summary>
    Task<List<StoredIdAndPosition>> GetCrashedReplicaMessages(IReadOnlySet<ReplicaId> liveReplicas);

    /// <summary>
    /// Re-assigns the messages at the provided positions to <paramref name="newReplica"/>,
    /// but only those still owned by <paramref name="expectedReplica"/>.
    /// </summary>
    Task SetReplica(IEnumerable<long> positions, ReplicaId newReplica, ReplicaId expectedReplica);
}