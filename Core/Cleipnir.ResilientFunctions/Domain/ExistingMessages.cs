using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;

namespace Cleipnir.ResilientFunctions.Domain;

/// <summary>
/// A flow's not-yet-delivered messages live in three carriers: rows in the message store, per-message
/// staged-message children under the queue manager's reserved root, and - for messages that arrived while the
/// flow was completed - the pending-messages entry inlined into the flow's effect state. This type surfaces the
/// union of all three, so control-panel tooling sees the same pending messages a restarted flow would receive.
///
/// Edits are effect-state-only: appended messages are written directly as row-less staged-message children
/// (never as store rows), so they cannot race the MessageWatchdog's row-to-effect inlining. Effect writes are
/// version-guarded, serializing them against concurrent inliner writes and claims. Consequently edits require
/// the flow to be unowned - editing a currently-executing flow fails with a concurrent-modification error.
/// </summary>
public class ExistingMessages
{
    private readonly StoredId _storedId;
    private List<StoredMessage>? _pendingMessages;
    private readonly IMessageStore _messageStore;
    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;

    public Task<IReadOnlyList<MessageAndIdempotencyKey>> MessagesWithIdempotencyKeys => GetPendingMessages()
        .ContinueWith(t => (IReadOnlyList<MessageAndIdempotencyKey>) t.Result.ToList());
    public Task<IReadOnlyList<object>> AsObjects => GetPendingMessages()
        .ContinueWith(t => (IReadOnlyList<object>) t.Result.Select(m => m.Message).ToList());
    public Task<int> Count => GetPendingMessages().SelectAsync(messages => messages.Count);

    public ExistingMessages(StoredId storedId, IMessageStore messageStore, IFunctionStore functionStore, ISerializer serializer)
    {
        _storedId = storedId;
        _messageStore = messageStore;
        _functionStore = functionStore;
        _serializer = serializer;
    }

    private async Task<List<MessageAndIdempotencyKey>> GetPendingMessages()
    {
        if (_pendingMessages is not null)
            return _pendingMessages.Select(m =>
                new MessageAndIdempotencyKey(
                    _serializer.Deserialize(m.MessageContent, _serializer.ResolveType(m.MessageType)!),
                    m.IdempotencyKey
                )
            ).ToList();

        _pendingMessages = await GetMergedMessages();
        return await GetPendingMessages();
    }

    // The union of all carriers, ordered by position. Row-backed children take their positions from the staged-
    // positions entry; row-less children get synthetic negative positions, so the view shows the exact delivery
    // order: control-panel appended messages first (in child order), then row-backed messages by store position.
    // Positions of the row-backed carriers are disjoint by construction (inlining deletes the row); should both
    // transiently hold a position, the store row wins.
    private async Task<List<StoredMessage>> GetMergedMessages()
    {
        var effects = (await _functionStore.GetFunction(_storedId))?.Effects ?? [];

        var byPosition = new Dictionary<long, StoredMessage>();
        foreach (var pending in DecodePendingInlinedMessages(effects))
            byPosition[pending.Position] = pending;

        foreach (var message in DecodeStagedMessageChildren(effects))
            byPosition.TryAdd(message.Position, message);

        // Empty messages are restart-pokes without payload - they are never delivered to the flow, so they are
        // not surfaced here either (and they have no content to deserialize).
        var storedMessages = await _messageStore.GetMessages(_storedId);
        foreach (var storedMessage in storedMessages.Where(m => !m.IsEmpty))
            byPosition[storedMessage.Position] = storedMessage;

        return byPosition.Values.OrderBy(m => m.Position).ToList();
    }

    private async Task<List<StoredMessage>> GetPendingInlinedMessages()
    {
        var effects = (await _functionStore.GetFunction(_storedId))?.Effects ?? [];
        return DecodePendingInlinedMessages(effects);
    }

    private static List<StoredMessage> DecodePendingInlinedMessages(IReadOnlyList<StoredEffect> effects)
    {
        var entry = effects.FirstOrDefault(e => e.EffectId == PendingMessages.EffectId);
        return entry?.Result is { Length: > 0 } bytes
            ? PendingMessages.Decode(bytes).Select(kv => kv.Value.ToStoredMessage(kv.Key)).ToList()
            : [];
    }

    private List<StoredMessage> DecodeStagedMessageChildren(IReadOnlyList<StoredEffect> effects)
    {
        var stagedPositions = DecodeStagedPositions(effects);
        var messages = new List<StoredMessage>();
        foreach (var effect in effects)
        {
            if (!QueueManager.StagedMessagesRoot.IsChild(effect.EffectId) || effect.Result == null)
                continue;

            var encoded = (byte[]) _serializer.Deserialize(effect.Result, typeof(byte[]));
            var incomingMessage = PendingMessages.DecodeMessage(encoded);
            // A row-backed child's position lives in the staged-positions entry; a row-less child gets a
            // synthetic negative position derived from its child index - keeps the view, delivery order and
            // Remove addressing consistent.
            var message = stagedPositions.TryGetValue(effect.EffectId.Id, out var storePosition)
                ? incomingMessage.ToStoredMessage(storePosition)
                : incomingMessage.ToStoredMessage(position: null) with { Position = long.MinValue + effect.EffectId.Id };
            messages.Add(message);
        }

        return messages;
    }

    private Dictionary<int, long> DecodeStagedPositions(IReadOnlyList<StoredEffect> effects)
    {
        var entry = effects.FirstOrDefault(e => e.EffectId == QueueManager.StagedPositionsId);
        return entry?.Result is { Length: > 0 } bytes
            ? QueueManager.ParseStagedPositions((List<long>) _serializer.Deserialize(bytes, typeof(List<long>)))
            : new Dictionary<int, long>();
    }

    public async Task Clear()
    {
        // Deleting the carriers is not atomic, and a MessageWatchdog holding rows fetched before the truncate
        // may concurrently move them into the pending-messages entry - delete, then verify after a grace period
        // and repeat until all carriers are observed empty. Besides the message carriers the flow's message
        // bookkeeping (delivered positions and idempotency keys) is wiped too, so messages re-appended with a
        // previously used idempotency key are not silently deduped away.
        for (var attempt = 0; attempt < 5; attempt++)
        {
            await _messageStore.Truncate(_storedId);

            var storedFlow = await _functionStore.GetFunction(_storedId);
            if (storedFlow == null)
                break;

            var effects = storedFlow.Effects ?? [];
            var deletes = effects
                .Where(e => IsMessageStateEffect(e.EffectId))
                .Select(e => StoredEffectChange.CreateDelete(_storedId, e.EffectId))
                .ToList();

            if (deletes.Count > 0)
            {
                var session = new SnapshotStorageSession { Version = storedFlow.Version };
                foreach (var effect in effects)
                    session.Effects[effect.EffectId] = effect;
                try
                {
                    await _functionStore.SetEffectResults(_storedId, deletes, owner: null, session);
                }
                catch (UnexpectedStateException)
                {
                    // Version or owner guard failed - another writer or a claim got in between; retry from a
                    // fresh read.
                }
            }

            await Task.Delay(100);
            if ((await GetMergedMessages()).Count == 0)
                break;
        }

        _pendingMessages = null;
    }

    private static bool IsMessageStateEffect(EffectId effectId)
        => effectId == PendingMessages.EffectId
           || effectId == QueueManager.DeliveredPositionsId
           || effectId == QueueManager.StagedPositionsId
           || effectId.IsDescendant(QueueManager.StagedMessagesRoot)
           || effectId.IsDescendant(QueueManager.IdempotencyKeysRoot);

    public Task Append<T>(T message, string? idempotencyKey = null) where T : notnull
        => WriteStagedMessageChild(
            EncodeRowlessMessage(message, idempotencyKey),
            chooseChildId: effects =>
            {
                var nextIndex = 0;
                foreach (var effect in effects)
                    if (QueueManager.StagedMessagesRoot.IsChild(effect.EffectId) && effect.EffectId.Id >= nextIndex)
                        nextIndex = effect.EffectId.Id + 1;
                return QueueManager.StagedMessagesRoot.CreateChild(nextIndex);
            }
        );

    /// <summary>
    /// Replaces the message at the provided position in the merged view. Only messages appended directly into
    /// the flow's effect state can be replaced - they have no store row a concurrent inliner could resurrect
    /// stale content from, and replacing the child in place preserves the message's delivery order. Row-backed
    /// messages must be removed and re-appended instead.
    /// </summary>
    /// <param name="position">Index of the message in the merged view</param>
    /// <param name="message">Replacement message</param>
    /// <param name="idempotencyKey">Replacement idempotency key</param>
    public async Task Replace<T>(int position, T message, string? idempotencyKey = null) where T : notnull
    {
        if (_pendingMessages is null)
            await GetPendingMessages();

        var storedMessage = _pendingMessages!.OrderBy(m => m.Position).Skip(position).FirstOrDefault();
        if (storedMessage == null)
            throw new ArgumentException($"Cannot replace non-existing message. Position '{position}' is larger than or equal to length '{_pendingMessages!.Count}'", nameof(position));
        if (storedMessage.RowBacked)
            throw new InvalidOperationException(
                "Only messages appended directly into the flow's effect state can be replaced - remove and re-append the message instead"
            );

        var childId = QueueManager.StagedMessagesRoot.CreateChild((int) (storedMessage.Position - long.MinValue));
        await WriteStagedMessageChild(
            EncodeRowlessMessage(message, idempotencyKey),
            chooseChildId: effects => effects.Any(e => e.EffectId == childId)
                ? childId
                // The child disappeared since the merged view was read (delivered by a concurrent incarnation or
                // removed by other tooling) - recreating it would resurrect a consumed message.
                : throw UnexpectedStateException.ConcurrentModification(_storedId)
        );
    }

    // Row-less: the message is written directly into the flow's effect state as a staged-message child and
    // never touches the message store - the QueueManager assigns it a synthetic position at staging.
    private byte[] EncodeRowlessMessage<T>(T message, string? idempotencyKey) where T : notnull
    {
        var json = _serializer.Serialize(message, message.GetType());
        var type = _serializer.SerializeType(message.GetType());
        var incomingMessage = new IncomingMessage(json, type, IdempotencyKey: idempotencyKey);
        return PendingMessages.EncodeMessage(incomingMessage);
    }

    private async Task WriteStagedMessageChild(byte[] encodedMessage, Func<IReadOnlyList<StoredEffect>, EffectId> chooseChildId)
    {
        for (var attempt = 0; ; attempt++)
        {
            var storedFlow = await _functionStore.GetFunction(_storedId);
            if (storedFlow == null)
                throw UnexpectedStateException.NotFound(_storedId);

            var effects = storedFlow.Effects ?? [];
            var childId = chooseChildId(effects);
            var entry = StoredEffect.CreateCompleted(childId, _serializer.Serialize(encodedMessage, typeof(byte[])), alias: null);
            var session = new SnapshotStorageSession { Version = storedFlow.Version };
            foreach (var effect in effects)
                session.Effects[effect.EffectId] = effect;

            try
            {
                await _functionStore.SetEffectResult(
                    _storedId,
                    new StoredEffectChange(_storedId, childId, CrudOperation.Insert, entry),
                    owner: null,
                    session
                );

                _pendingMessages = null;
                return;
            }
            catch (UnexpectedStateException) when (attempt < 5)
            {
                // Version or owner guard failed - another writer or a claim got in between; retry from a fresh
                // read (which also re-evaluates the target child id).
            }
        }
    }

    /// <summary>
    /// Removes the message at the provided position.
    /// </summary>
    /// <param name="position">Message position</param>
    public async Task Remove(long position)
    {
        if (position < 0)
        {
            // A synthetic position addresses a row-less staged-message child - delete the child effect itself.
            var childId = QueueManager.StagedMessagesRoot.CreateChild((int) (position - long.MinValue));
            await _functionStore.DeleteEffectResult(_storedId, childId, owner: null, storageSession: null);
        }
        else
        {
            await _messageStore.DeleteMessages(positions: [position]);

            var pending = await GetPendingInlinedMessages();
            if (pending.Any(m => m.Position == position))
                await WritePendingInlinedMessages(pending.Where(m => m.Position != position).ToList());
        }

        // Invalidate cache so it will be re-fetched with correct data
        _pendingMessages = null;
    }

    private async Task WritePendingInlinedMessages(IReadOnlyList<StoredMessage> messages)
    {
        if (messages.Count == 0)
        {
            await _functionStore.DeleteEffectResult(_storedId, PendingMessages.EffectId, owner: null, storageSession: null);
            return;
        }

        var entry = StoredEffect.CreateCompleted(
            PendingMessages.EffectId,
            PendingMessages.Encode(messages.ToDictionary(m => m.Position, IncomingMessage.From)),
            alias: null
        );
        await _functionStore.SetEffectResult(
            _storedId,
            new StoredEffectChange(_storedId, PendingMessages.EffectId, CrudOperation.Insert, entry),
            owner: null,
            session: null
        );
    }
}
