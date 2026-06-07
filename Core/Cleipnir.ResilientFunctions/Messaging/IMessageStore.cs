using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public interface IMessageStore
{
    Task Initialize();

    /// <summary>
    /// Appends a message to the target flow and returns the replica written to the message row
    /// (the target flow's current owner, or the publishing replica when the target is not executing).
    /// The target flow is scheduled to run immediately when it is suspended/postponed.
    /// </summary>
    Task<ReplicaId> AppendMessage(StoredId storedId, StoredMessage storedMessage);
    Task AppendMessages(IReadOnlyList<StoredIdAndMessage> messages);

    Task<bool> ReplaceMessage(StoredId storedId, long position, StoredMessage storedMessage);
    Task DeleteMessages(StoredId storedId, IEnumerable<long> positions);

    Task Truncate(StoredId storedId);

    Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId);
    Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, IReadOnlyList<long> skipPositions);
    Task<Dictionary<StoredId, List<StoredMessage>>> GetMessages(IEnumerable<StoredId> storedIds);

    /// <summary>
    /// Returns the undelivered messages whose replica equals the provided replica, grouped by target flow.
    /// Used by the MessageWatchdog to push messages to live flows owned by this replica.
    /// </summary>
    Task<Dictionary<StoredId, List<StoredMessage>>> GetMessagesForReplica(ReplicaId replicaId);
}