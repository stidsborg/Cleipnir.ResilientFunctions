using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryDlqStore : IDlqStore
{
    private readonly Dictionary<long, StoredMessage> _messages = new();
    private long _nextPosition;
    private readonly Lock _sync = new();

    public Task Initialize() => Task.CompletedTask;

    public Task Append(IReadOnlyList<StoredMessage> messages)
    {
        lock (_sync)
            foreach (var message in messages)
                _messages[_nextPosition++] = message;

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<StoredDlqMessage>> GetMessages(long? offset = null, int limit = 1_000)
    {
        lock (_sync)
            return _messages
                .Where(kv => kv.Key > (offset ?? -1))
                .OrderBy(kv => kv.Key)
                .Take(limit)
                .Select(kv => ToDlqMessage(kv.Key, kv.Value))
                .ToList()
                .CastTo<IReadOnlyList<StoredDlqMessage>>()
                .ToTask();
    }

    public Task<IReadOnlyList<StoredDlqMessage>> GetMessages(IReadOnlyList<StoredId> storedIds)
    {
        var ids = storedIds.ToHashSet();
        lock (_sync)
            return _messages
                .Where(kv => ids.Contains(kv.Value.StoredId))
                .OrderBy(kv => kv.Key)
                .Select(kv => ToDlqMessage(kv.Key, kv.Value))
                .ToList()
                .CastTo<IReadOnlyList<StoredDlqMessage>>()
                .ToTask();
    }

    public Task<IReadOnlyList<StoredDlqMessage>> GetMessages(IReadOnlyList<long> positions)
    {
        var positionsToFetch = positions.ToHashSet();
        lock (_sync)
            return _messages
                .Where(kv => positionsToFetch.Contains(kv.Key))
                .OrderBy(kv => kv.Key)
                .Select(kv => ToDlqMessage(kv.Key, kv.Value))
                .ToList()
                .CastTo<IReadOnlyList<StoredDlqMessage>>()
                .ToTask();
    }

    private static StoredDlqMessage ToDlqMessage(long position, StoredMessage message)
    {
        return new StoredDlqMessage(
            message.StoredId,
            position,
            message.MessageContent,
            // Dead lettered messages always carry a payload - empty restart-pokes are filtered out before append.
            message.MessageType!.Value,
            message.IdempotencyKey,
            message.Sender,
            message.Receiver
        );
    }

    public Task Delete(IReadOnlyList<long> positions)
    {
        lock (_sync)
            foreach (var position in positions)
                _messages.Remove(position);

        return Task.CompletedTask;
    }
}
