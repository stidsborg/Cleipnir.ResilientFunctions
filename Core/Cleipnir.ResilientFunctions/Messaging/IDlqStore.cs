using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public interface IDlqStore
{
    Task Initialize();

    /// <summary>
    /// Appends the messages to the dead letter queue. Each row is assigned a globally unique dlq position
    /// (identity value) in caller order; the messages' incoming <see cref="StoredMessage.Position"/> values are
    /// not persisted - fetched messages carry their dlq position instead.
    /// </summary>
    Task Append(IReadOnlyList<StoredIdAndMessage> messages);

    Task<IReadOnlyList<StoredDlqMessage>> GetMessages();
    Task<IReadOnlyList<StoredDlqMessage>> GetMessages(IReadOnlyList<StoredId> storedIds);

    /// <summary>
    /// Deletes the messages at the given dlq positions regardless of which flow they belong to. Dlq positions
    /// are globally unique (identity values), so no <see cref="StoredId"/> is needed.
    /// </summary>
    Task Delete(IReadOnlyList<long> positions);
}
