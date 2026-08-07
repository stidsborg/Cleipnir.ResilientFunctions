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
/// Control-panel view over a flow's admitted-but-undelivered messages: the staged-message children in the
/// flow's effect state. Effect state is the single carrier the view reads and edits - messages still in transit
/// (store rows the QueueManager has not admitted yet) and dead lettered messages (see DlqManager) are
/// deliberately not surfaced here.
///
/// Appended messages are written directly as row-less staged-message children (never as store rows), so they
/// cannot race the MessageWatchdog's fetch-and-push cycle. Effect writes are version-guarded, serializing them
/// against claims. Consequently edits require the flow to be unowned - editing a currently-executing flow fails
/// with a concurrent-modification error. Removing or clearing a row-backed staged message also deletes its
/// backing store row, so a later fetch cannot re-admit the removed message.
/// </summary>
public class ExistingMessages
{
    private readonly StoredId _storedId;
    private List<StagedMessageChild>? _stagedMessages;
    private readonly IMessageStore _messageStore;
    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;
    private readonly TypeMapper _typeMapper;

    public Task<IReadOnlyList<MessageAndIdempotencyKey>> MessagesWithIdempotencyKeys => GetDeserializedMessages()
        .ContinueWith(t => (IReadOnlyList<MessageAndIdempotencyKey>) t.Result.ToList());
    public Task<IReadOnlyList<object>> AsObjects => GetDeserializedMessages()
        .ContinueWith(t => (IReadOnlyList<object>) t.Result.Select(m => m.Message).ToList());
    public Task<int> Count => GetStagedMessages().SelectAsync(messages => messages.Count);

    public ExistingMessages(StoredId storedId, IFunctionStore functionStore, ISerializer serializer, TypeMapper typeMapper)
    {
        _storedId = storedId;
        _messageStore = functionStore.MessageStore;
        _functionStore = functionStore;
        _serializer = serializer;
        _typeMapper = typeMapper;
    }

    private async Task<List<MessageAndIdempotencyKey>> GetDeserializedMessages()
    {
        var stagedMessages = await GetStagedMessages();
        return stagedMessages.Select(staged =>
            new MessageAndIdempotencyKey(
                _serializer.Deserialize(staged.Message.MessageContent, _typeMapper.ResolveType(staged.Message.MessageType!.Value)),
                staged.Message.IdempotencyKey
            )
        ).ToList();
    }

    // The flow's staged-message children ordered by position: row-less children carry the same synthetic
    // negative positions the QueueManager assigns at staging, so control-panel appended messages come first
    // (in child order), then the store-addressed messages by their row position - matching delivery order.
    private async Task<List<StagedMessageChild>> GetStagedMessages()
    {
        if (_stagedMessages is not null)
            return _stagedMessages;

        var effects = (await _functionStore.GetFunction(_storedId))?.Effects ?? [];

        var messages = new List<StagedMessageChild>();
        foreach (var effect in effects)
        {
            if (!QueueManager.StagedMessagesRoot.IsChild(effect.EffectId) || effect.Result == null)
                continue;

            var encoded = (byte[]) _serializer.Deserialize(effect.Result, typeof(byte[]));
            var message = PendingMessages.DecodeMessage(encoded, _storedId);
            // Same synthetic-position formula as the QueueManager's staging - keeps the view, delivery order and
            // Remove addressing consistent.
            if (!message.RowBacked)
                message = message with { Position = long.MinValue + effect.EffectId.Id };
            messages.Add(new StagedMessageChild(effect.EffectId, message));
        }

        _stagedMessages = messages.OrderBy(m => m.Message.Position).ToList();
        return _stagedMessages;
    }

    public async Task Clear()
    {
        // Rows before effects: dying in between leaves the staged children visible and a retried Clear starts
        // over, whereas effects-first would leave the row-backed messages' store rows behind to be re-fetched
        // and re-admitted. Truncate rather than per-position deletes, so delivered-but-not-yet-cleared rows
        // cannot resurrect once the delivered-positions bookkeeping is wiped below. Besides the staged children
        // the flow's message bookkeeping (delivered positions and idempotency keys) is wiped too, so messages
        // re-appended with a previously used idempotency key are not silently deduped away.
        for (var attempt = 0; ; attempt++)
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
            if (deletes.Count == 0)
                break;

            var session = new SnapshotStorageSession { Version = storedFlow.Version };
            foreach (var effect in effects)
                session.Effects[effect.EffectId] = effect;
            try
            {
                await _functionStore.SetEffectResults(_storedId, deletes, owner: null, session);
                break;
            }
            catch (UnexpectedStateException) when (attempt < 5)
            {
                // Version or owner guard failed - another writer or a claim got in between; retry from a
                // fresh read.
            }
        }

        _stagedMessages = null;
    }

    private static bool IsMessageStateEffect(EffectId effectId)
        => effectId == QueueManager.DeliveredPositionsId
           || effectId.IsDescendant(QueueManager.StagedMessagesRoot)
           || effectId.IsDescendant(QueueManager.IdempotencyKeysRoot);

    public Task Append<T>(T message, string? idempotencyKey = null) where T : notnull
        => WriteStagedMessageChild(
            EncodeMessage(message, idempotencyKey, position: null),
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
    /// Replaces the message at the provided position in the view by overwriting its staged-message child effect
    /// in place, preserving the message's delivery order. A row-backed message's replacement keeps the original
    /// store position, so the still-present store row stays deduped against re-admission and is deleted as usual
    /// once the replacement is delivered.
    /// </summary>
    /// <param name="position">Index of the message in the view</param>
    /// <param name="message">Replacement message</param>
    /// <param name="idempotencyKey">Replacement idempotency key</param>
    public async Task Replace<T>(int position, T message, string? idempotencyKey = null) where T : notnull
    {
        var stagedMessages = await GetStagedMessages();
        var target = stagedMessages.Skip(position).FirstOrDefault();
        if (target == null)
            throw new ArgumentException($"Cannot replace non-existing message. Position '{position}' is larger than or equal to length '{stagedMessages.Count}'", nameof(position));

        var childId = target.ChildId;
        await WriteStagedMessageChild(
            EncodeMessage(message, idempotencyKey, target.Message.RowBacked ? target.Message.Position : null),
            chooseChildId: effects => effects.Any(e => e.EffectId == childId)
                ? childId
                // The child disappeared since the view was read (delivered by a concurrent incarnation or
                // removed by other tooling) - recreating it would resurrect a consumed message.
                : throw UnexpectedStateException.ConcurrentModification(_storedId)
        );
    }

    // Appended messages are row-less (position null): written directly into the flow's effect state as a
    // staged-message child, never touching the message store - the QueueManager assigns a synthetic position at
    // staging. A replacement for a row-backed message carries the original store position instead.
    private byte[] EncodeMessage<T>(T message, string? idempotencyKey, long? position) where T : notnull
    {
        var json = _serializer.Serialize(message, message.GetType());
        var type = _typeMapper.GetTypeId(message.GetType());
        return PendingMessages.EncodeMessage(json, type, position, idempotencyKey: idempotencyKey);
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
            var entry = StoredEffect.CreateCompleted(
                childId,
                _serializer.Serialize(encodedMessage, typeof(byte[])),
                _typeMapper.GetTypeId(typeof(byte[])),
                alias: null
            );
            var session = new SnapshotStorageSession { Version = storedFlow.Version };
            foreach (var effect in effects)
                session.Effects[effect.EffectId] = effect;

            try
            {
                await _typeMapper.EnsurePersisted();
                await _functionStore.SetEffectResult(
                    _storedId,
                    new StoredEffectChange(_storedId, childId, CrudOperation.Insert, entry),
                    owner: null,
                    session
                );

                _stagedMessages = null;
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
        var stagedMessages = await GetStagedMessages();
        var target = stagedMessages.FirstOrDefault(staged => staged.Message.Position == position);
        _stagedMessages = null;
        if (target == null)
            return;

        // Row before child: dying in between leaves the child visible and Remove retryable, whereas child-first
        // would leave the row behind to be re-fetched and re-admitted as a fresh staged message.
        if (target.Message.RowBacked)
            await _messageStore.DeleteMessages(positions: [position]);

        await _functionStore.DeleteEffectResult(_storedId, target.ChildId, owner: null, storageSession: null);
    }

    // A staged-message child paired with the id it lives under - Remove and Replace address the child by it.
    private record StagedMessageChild(EffectId ChildId, StoredMessage Message);
}
