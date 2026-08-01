using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryDlqStore : IDlqStore
{
    private readonly Dictionary<long, StoredIdAndMessage> _messages = new();
    private long _nextPosition;
    private readonly Lock _sync = new();

    public Task Initialize() => Task.CompletedTask;

    public Task Append(IReadOnlyList<StoredIdAndMessage> messages)
    {
        lock (_sync)
            foreach (var message in messages)
                _messages[_nextPosition++] = message;

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<StoredDlqMessage>> GetMessages()
    {
        lock (_sync)
            return _messages
                .OrderBy(kv => kv.Key)
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

    private static StoredDlqMessage ToDlqMessage(long position, StoredIdAndMessage stored)
    {
        var (storedId, message) = stored;
        return new StoredDlqMessage(
            storedId,
            position,
            message.MessageContent,
            message.MessageType,
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
