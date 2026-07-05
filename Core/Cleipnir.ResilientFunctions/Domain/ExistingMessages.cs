using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

/// <summary>
/// A flow's not-yet-delivered messages live in one of two carriers: rows in the message store, or - for messages
/// that arrived while the flow was completed - per-message child effects inlined into the flow's effect state.
/// This type surfaces and edits the union of both, so control-panel tooling sees the same pending messages a
/// restarted flow would receive.
/// </summary>
public class ExistingMessages
{
    private readonly StoredId _storedId;
    private List<StoredMessage>? _receivedMessages;
    private readonly IMessageStore _messageStore;
    private readonly IEffectsStore _effectsStore;
    private readonly ISerializer _serializer;
    private readonly ReplicaId _publisherReplica;

    public Task<IReadOnlyList<MessageAndIdempotencyKey>> MessagesWithIdempotencyKeys => GetReceivedMessages()
        .ContinueWith(t => (IReadOnlyList<MessageAndIdempotencyKey>) t.Result.ToList());
    public Task<IReadOnlyList<object>> AsObjects => GetReceivedMessages()
        .ContinueWith(t => (IReadOnlyList<object>) t.Result.Select(m => m.Message).ToList());
    public Task<int> Count => GetReceivedMessages().SelectAsync(messages => messages.Count);

    public ExistingMessages(StoredId storedId, IMessageStore messageStore, IEffectsStore effectsStore, ISerializer serializer, ReplicaId publisherReplica)
    {
        _storedId = storedId;
        _messageStore = messageStore;
        _effectsStore = effectsStore;
        _serializer = serializer;
        _publisherReplica = publisherReplica;
    }

    private async Task<List<MessageAndIdempotencyKey>> GetReceivedMessages()
    {
        if (_receivedMessages is not null)
            return _receivedMessages.Select(m =>
                new MessageAndIdempotencyKey(
                    _serializer.Deserialize(m.MessageContent, _serializer.ResolveType(m.MessageType)!),
                    m.IdempotencyKey
                )
            ).ToList();

        _receivedMessages = await GetMergedMessages();
        return await GetReceivedMessages();
    }

    // The union of both carriers, ordered by position. Positions are disjoint by construction (inlining deletes
    // the row); should both carriers transiently hold a position, the store row wins.
    private async Task<List<StoredMessage>> GetMergedMessages()
    {
        var byPosition = new Dictionary<long, StoredMessage>();
        foreach (var pending in await GetPendingInlinedMessages())
            byPosition[pending.Position] = pending;

        // Empty messages are restart-pokes without payload - they are never delivered to the flow, so they are
        // not surfaced here either (and they have no content to deserialize).
        var storedMessages = await _messageStore.GetMessages(_storedId);
        foreach (var storedMessage in storedMessages.Where(m => !m.IsEmpty))
            byPosition[storedMessage.Position] = storedMessage;

        return byPosition.Values.OrderBy(m => m.Position).ToList();
    }

    private async Task<List<StoredMessage>> GetPendingInlinedMessages()
    {
        var effects = await _effectsStore.GetEffectResults(_storedId);
        return effects
            .Where(e => PendingMessages.Root.IsChild(e.EffectId) && e.Result is { Length: > 0 })
            .Select(e => PendingMessages.DecodeMessage(e.Result!))
            .OrderBy(m => m.Position)
            .ToList();
    }

    public async Task Clear()
    {
        // Deleting the two carriers is not atomic, and a MessageWatchdog holding rows fetched before the truncate
        // may concurrently move them into the pending-messages children - delete, then verify after a grace period
        // and repeat until both carriers are observed empty.
        for (var attempt = 0; attempt < 5; attempt++)
        {
            await _messageStore.Truncate(_storedId);
            var pending = await GetPendingInlinedMessages();
            if (pending.Count > 0)
                await DeletePendingInlinedMessages(pending.Select(m => m.Position));

            await Task.Delay(100);
            if ((await GetMergedMessages()).Count == 0)
                break;
        }

        _receivedMessages = null;
    }

    public async Task Append<T>(T message, string? idempotencyKey = null) where T : notnull
    {
        var json = _serializer.Serialize(message, message.GetType());
        var type = _serializer.SerializeType(message.GetType());
        // Stamped with this replica so the message is routed to this replica's MessageWatchdog - the sole
        // message-delivery path; a ReplicaId.Empty stamp would make the message invisible to every watchdog.
        var storedMessage = new StoredMessage(json, type, Position: 0, Replica: _publisherReplica, IdempotencyKey: idempotencyKey);
        await _messageStore.AppendMessages([new StoredIdAndMessage(_storedId, storedMessage)]);

        // Invalidate cache so it will be re-fetched with correct positions
        _receivedMessages = null;
    }

    public async Task Replace<T>(int position, T message, string? idempotencyKey = null) where T : notnull
    {
        if (_receivedMessages is null)
            await GetReceivedMessages();

        var storedMessage = _receivedMessages!.OrderBy(m => m.Position).Skip(position).FirstOrDefault();
        if (storedMessage == null)
            throw new ArgumentException($"Cannot replace non-existing message. Position '{position}' is larger than or equal to length '{_receivedMessages!.Count}'", nameof(position));

        var json = _serializer.Serialize(message, message.GetType());
        var type = _serializer.SerializeType(message.GetType());
        var replacement = new StoredMessage(json, type, Position: storedMessage.Position, Replica: _publisherReplica, IdempotencyKey: idempotencyKey);

        // The row may have been inlined into the effect state (and deleted) since the message was read - when the
        // store replace misses, upsert the replacement into the pending-messages entry instead.
        var replaced = await _messageStore.ReplaceMessage(_storedId, storedMessage.Position, replacement);
        if (!replaced)
            await UpsertPendingInlinedMessage(replacement);

        // Invalidate cache so it will be re-fetched with correct data
        _receivedMessages = null;
    }

    /// <summary>
    /// Removes the message at the provided position.
    /// </summary>
    /// <param name="position">Message position</param>
    public async Task Remove(long position)
    {
        await _messageStore.DeleteMessages(positions: [position]);
        await _effectsStore.DeleteEffectResult(_storedId, PendingMessages.ChildId(position), storageSession: null);

        // Invalidate cache so it will be re-fetched with correct data
        _receivedMessages = null;
    }

    private async Task UpsertPendingInlinedMessage(StoredMessage message)
    {
        var childId = PendingMessages.ChildId(message.Position);
        var entry = StoredEffect.CreateCompleted(childId, PendingMessages.EncodeMessage(message), alias: null);
        await _effectsStore.SetEffectResult(
            _storedId,
            new StoredEffectChange(_storedId, childId, CrudOperation.Insert, entry),
            session: null
        );
    }

    private async Task DeletePendingInlinedMessages(IEnumerable<long> positions)
    {
        var changes = positions
            .Select(p => new StoredEffectChange(_storedId, PendingMessages.ChildId(p), CrudOperation.Delete, StoredEffect: null))
            .ToList();
        if (changes.Count > 0)
            await _effectsStore.SetEffectResults(_storedId, changes, session: null);
    }
}
