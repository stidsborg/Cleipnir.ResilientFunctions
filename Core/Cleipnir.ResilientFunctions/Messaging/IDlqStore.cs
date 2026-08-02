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
    /// not persisted - fetched messages carry their dlq position in <see cref="StoredDlqMessage.Position"/>.
    /// </summary>
    Task Append(IReadOnlyList<StoredMessage> messages);

    /// <summary>
    /// Fetches at most <paramref name="limit"/> dead lettered messages ordered by dlq position, starting after
    /// the <paramref name="offset"/> dlq position (exclusive) or at the beginning of the queue when omitted.
    /// Page through the queue by passing the last returned position as the next offset.
    /// </summary>
    Task<IReadOnlyList<StoredDlqMessage>> GetMessages(long? offset = null, int limit = 1_000);
    Task<IReadOnlyList<StoredDlqMessage>> GetMessages(IReadOnlyList<StoredId> storedIds);

    /// <summary>
    /// Fetches the messages at the given dlq positions regardless of which flow they belong to. Positions
    /// without a matching row are silently skipped.
    /// </summary>
    Task<IReadOnlyList<StoredDlqMessage>> GetMessages(IReadOnlyList<long> positions);

    /// <summary>
    /// Deletes the messages at the given dlq positions regardless of which flow they belong to. Dlq positions
    /// are globally unique (identity values), so no <see cref="StoredId"/> is needed.
    /// </summary>
    Task Delete(IReadOnlyList<long> positions);
}
