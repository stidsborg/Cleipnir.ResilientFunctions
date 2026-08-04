using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

/// <summary>
/// Facade over the dead letter queue used both by the framework itself and by external users inspecting,
/// removing or redriving dead lettered messages. Obtained from
/// <see cref="FunctionsRegistry.DeadLetterQueue"/>.
/// </summary>
public class DlqManager
{
    private readonly IDlqStore _dlqStore;
    private readonly MessageSender _messageSender;
    private readonly StoredTypes _storedTypes;
    private readonly IMessageClearer _messageClearer;
    private readonly UnhandledExceptionHandler _unhandledExceptionHandler;
    private readonly TimeSpan _unregisteredFlowTypesGracePeriod;

    internal DlqManager(
        IDlqStore dlqStore,
        MessageSender messageSender,
        StoredTypes storedTypes,
        IMessageClearer messageClearer,
        UnhandledExceptionHandler unhandledExceptionHandler,
        TimeSpan unregisteredFlowTypesGracePeriod)
    {
        _dlqStore = dlqStore;
        _messageSender = messageSender;
        _storedTypes = storedTypes;
        _messageClearer = messageClearer;
        _unhandledExceptionHandler = unhandledExceptionHandler;
        _unregisteredFlowTypesGracePeriod = unregisteredFlowTypesGracePeriod;
    }

    /// <summary>
    /// Fetches at most <paramref name="limit"/> dead lettered messages ordered by dlq position, starting after
    /// the <paramref name="offset"/> dlq position (exclusive) or at the beginning of the queue when omitted.
    /// Page through the queue by passing the last returned position as the next offset.
    /// </summary>
    public Task<IReadOnlyList<StoredDlqMessage>> GetMessages(long? offset = null, int limit = 1_000)
        => _dlqStore.GetMessages(offset, limit);
    public Task<IReadOnlyList<StoredDlqMessage>> GetMessages(IReadOnlyList<StoredId> storedIds) => _dlqStore.GetMessages(storedIds);

    public Task Delete(IReadOnlyList<long> positions) => _dlqStore.Delete(positions);

    /// <summary>
    /// Redrives the dead lettered messages at the provided dlq positions.
    /// </summary>
    public async Task Redrive(IReadOnlyList<long> positions)
        => await Redrive(await _dlqStore.GetMessages(positions));

    /// <summary>
    /// Redrives all the dead lettered messages belonging to the provided flows.
    /// </summary>
    public async Task Redrive(IReadOnlyList<StoredId> storedIds)
        => await Redrive(await _dlqStore.GetMessages(storedIds));

    /// <summary>
    /// Redrives all the dead lettered messages belonging to the provided flows.
    /// </summary>
    public async Task Redrive(IReadOnlyList<FlowId> flowIds)
    {
        var storedIds = new List<StoredId>(flowIds.Count);
        foreach (var flowId in flowIds)
        {
            var storedType = await _storedTypes.InsertOrGet(flowId.Type);
            storedIds.Add(StoredId.Create(storedType, flowId.Instance.Value));
        }

        await Redrive(storedIds);
    }

    /// <summary>
    /// Moves the messages back into the message store, so they are delivered to their flows again. Each message
    /// is stamped with its flow's responsible replica - the original publisher replica is not retained - so the
    /// redelivery work is sharded across the cluster. Message-store append happens before the dlq delete, so a
    /// crash in between redrives the messages a second time rather than losing them.
    /// </summary>
    private async Task Redrive(IReadOnlyList<StoredDlqMessage> dlqMessages)
    {
        if (dlqMessages.Count == 0)
            return;

        await _messageSender.SendMessages(
            dlqMessages
                .Select(m => new SerializedMessage(
                    m.StoredId,
                    m.MessageContent,
                    m.MessageType,
                    m.IdempotencyKey,
                    m.Sender,
                    m.Receiver
                ))
                .ToList()
        );

        await _dlqStore.Delete(dlqMessages.Select(m => m.Position).ToList());
    }

    /// <summary>
    /// Holds the undeliverable messages for the configured grace period and then moves them to the dead letter
    /// queue. The messages' positions stay marked as pushed throughout the hold, so they are fetched exactly once
    /// - a message is either held here or in delivery, never both. Because flow types are only registered at
    /// registry-creation time, an undeliverable message can never become deliverable on this replica: the hold
    /// exists to give a rolling deployment time to recycle this replica - a process restart discards the hold,
    /// after which the messages are re-assigned to a replica that may have the type registered. Empty
    /// restart-pokes carry nothing to redrive and are simply deleted.
    /// </summary>
    internal void MoveToDlqAfterGracePeriod(IReadOnlyList<StoredMessage> undeliverable)
        => _ = HoldThenMove(undeliverable);

    private async Task HoldThenMove(IReadOnlyList<StoredMessage> undeliverable)
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

    // Dlq append before row delete, so a crash in between dead letters the messages a second time rather than
    // losing them. The final Clear covers every held position: it deletes the rows and trims the positions from
    // the watchdog's ignore-set.
    private async Task Move(IReadOnlyList<StoredMessage> undeliverable)
    {
        var nonEmpty = undeliverable
            .Where(m => !m.IsEmpty)
            .ToList();

        if (nonEmpty.Count > 0)
        {
            await _dlqStore.Append(nonEmpty);
            var storedTypes = nonEmpty.Select(m => m.StoredId.Type.Value).Distinct().ToList();
            _unhandledExceptionHandler.Invoke(
                new FrameworkException(
                    $"Moved {nonEmpty.Count} message(s) to the dead letter queue - their flow types (stored types: [{string.Join(", ", storedTypes)}]) were not registered on this replica"
                )
            );
        }

        await _messageClearer.Clear(
            undeliverable.Select(m => m.Position).ToList()
        );
    }
}
