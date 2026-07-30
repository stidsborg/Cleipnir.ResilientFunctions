using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

/// <summary>
/// Facade over the dead letter queue used both by the framework itself and by external users inspecting,
/// removing or (eventually) redriving dead lettered messages. Obtained from
/// <see cref="FunctionsRegistry.DeadLetterQueue"/>.
/// </summary>
public class DlqManager
{
    private readonly IDlqStore _dlqStore;
    private readonly IMessageStore _messageStore;
    private readonly IMessageClearer _messageClearer;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly TimeSpan _unregisteredFlowTypesGracePeriod;

    internal DlqManager(
        IDlqStore dlqStore,
        IMessageStore messageStore,
        IMessageClearer messageClearer,
        UnhandledExceptionHandler unhandledExceptionHandler,
        TimeSpan unregisteredFlowTypesGracePeriod)
    {
        _dlqStore = dlqStore;
        _messageStore = messageStore;
        _messageClearer = messageClearer;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _unregisteredFlowTypesGracePeriod = unregisteredFlowTypesGracePeriod;
    }

    public Task Append(IReadOnlyList<StoredIdAndMessage> messages) => _dlqStore.Append(messages);

    public Task<IReadOnlyList<StoredDlqMessage>> GetMessages() => _dlqStore.GetMessages();
    public Task<IReadOnlyList<StoredDlqMessage>> GetMessages(IReadOnlyList<StoredId> storedIds) => _dlqStore.GetMessages(storedIds);

    public Task Delete(IReadOnlyList<long> positions) => _dlqStore.Delete(positions);

    /// <summary>
    /// Holds the undeliverable messages for the configured grace period and then moves them to the dead letter
    /// queue. The messages' positions stay marked as pushed throughout the hold, so they are fetched exactly once
    /// - a message is either held here or in delivery, never both. Because flow types are only registered at
    /// registry-creation time, an undeliverable message can never become deliverable on this replica: the hold
    /// exists to give a rolling deployment time to recycle this replica - a process restart discards the hold,
    /// after which the messages are re-assigned to a replica that may have the type registered. Empty
    /// restart-pokes carry nothing to redrive and are simply deleted.
    /// </summary>
    internal void MoveToDlqAfterGracePeriod(IReadOnlyList<StoredMessages> undeliverable)
        => _ = HoldThenMove(undeliverable);

    private async Task HoldThenMove(IReadOnlyList<StoredMessages> undeliverable)
    {
        await Task.Delay(_unregisteredFlowTypesGracePeriod);

        while (true)
        {
            try
            {
                await Move(undeliverable);
                return;
            }
            catch (Exception exception)
            {
                _unhandledExceptionHandler.Invoke(
                    new FrameworkException(
                        "Failed to dead letter undeliverable messages - retrying",
                        innerException: exception
                    )
                );
                await Task.Delay(TimeSpan.FromSeconds(5));
            }
        }
    }

    // Dead letters from the store's CURRENT rows, not the held copies: control-panel tooling may have replaced
    // (stale content) or deleted rows during the hold - a deleted row must stay deleted and a replaced row must
    // be dead lettered with its fresh content. Dlq append before row delete, so a crash in between dead letters
    // the messages a second time rather than losing them. The final Clear covers every held position: it deletes
    // the surviving rows and trims all the positions from the watchdog's ignore-set (already-gone rows no-op).
    private async Task Move(IReadOnlyList<StoredMessages> undeliverable)
    {
        var expired = new List<StoredIdAndMessage>();
        foreach (var storedMessages in undeliverable)
        {
            var heldPositions = storedMessages.Messages.Select(m => m.Position).ToHashSet();
            var currentRows = await _messageStore.GetMessages(storedMessages.StoredId);
            expired.AddRange(
                currentRows
                    .Where(m => !m.IsEmpty && heldPositions.Contains(m.Position))
                    .Select(m => new StoredIdAndMessage(storedMessages.StoredId, m))
            );
        }

        if (expired.Count > 0)
        {
            await _dlqStore.Append(expired);
            var storedTypes = expired.Select(m => m.StoredId.Type.Value).Distinct().ToList();
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    $"Moved {expired.Count} message(s) to the dead letter queue - their flow types (stored types: [{string.Join(", ", storedTypes)}]) were not registered on this replica"
                )
            );
        }

        await _messageClearer.Clear(
            undeliverable.SelectMany(sm => sm.Messages).Select(m => m.Position).ToList()
        );
    }
}
