using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public interface IDlqStore
{
    Task Initialize();

    /// <summary>
    /// Appends the messages to the dead letter queue. Each row is assigned a globally unique dlq position
    /// (identity value) in caller order; the message's previous message-store position is persisted alongside
    /// the content for informational purposes.
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
