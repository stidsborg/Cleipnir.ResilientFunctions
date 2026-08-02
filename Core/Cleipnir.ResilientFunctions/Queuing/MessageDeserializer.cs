using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Queuing;

/// <summary>
/// Deserializes fetched messages into the object-form pipeline - at the fetch boundary (the MessageWatchdog's
/// fetch-and-push cycle) and when the <see cref="QueueManager"/> re-stages messages from their effect carriers
/// at initialization. A single registry-wide instance: the serializer is registry-global (per-registration
/// settings cannot override it), and every message carries its target flow. A message that fails to
/// deserialize never enters the pipeline: the failure is reported to the unhandled-exception handler and the
/// message is moved to the dead letter queue - appended to the dlq store and deleted from the message store - so
/// it can neither poison the flow nor be endlessly re-fetched. The move is at-least-once (dlq append before row
/// delete), so a crash in between dead letters the message a second time rather than losing it.
/// </summary>
internal class MessageDeserializer(
    ISerializer serializer,
    IDlqStore dlqStore,
    IMessageClearer messageClearer,
    UnhandledExceptionHandler unhandledExceptionHandler)
{
    /// <summary>
    /// Deserializes a single message, dead lettering it on failure. Returns the object-form message, or null
    /// when it was dead lettered - a caller staging from an in-flow carrier (a staged-message child effect)
    /// must then clear that carrier.
    /// </summary>
    public async Task<IncomingMessage?> DeserializeOrDeadLetter(StoredMessage message)
    {
        try
        {
            var payload = serializer.Deserialize(message.MessageContent, serializer.ResolveType(message.MessageType)!);
            return ToIncomingMessage(payload, message);
        }
        catch (Exception exception)
        {
            ReportDeserializationFailure(message.StoredId, exception);
            await MoveToDlq([message]);
            return null;
        }
    }

    private static IncomingMessage ToIncomingMessage(object payload, StoredMessage message)
        => new(
            message.StoredId,
            payload,
            message.RowBacked ? message.Position : null,
            message.IdempotencyKey,
            message.Sender,
            message.Receiver
        );

    private void ReportDeserializationFailure(StoredId storedId, Exception exception)
        => unhandledExceptionHandler.Invoke(
            new FrameworkException($"Message deserialization failed for flow '{storedId}' - the message is moved to the dead letter queue", exception)
        );

    private async Task MoveToDlq(List<StoredMessage> messages)
    {
        await dlqStore.Append(messages);

        // Row deletes come after the dlq append has landed - a crash in between re-fetches and re-dead-letters
        // the message rather than losing it. Row-less messages (e.g. control-panel appended)
        // have no row to delete; their in-flow carrier is pruned by the caller instead.
        var rowBackedPositions = messages
            .Where(message => message.RowBacked)
            .Select(message => message.Position)
            .ToList();
        if (rowBackedPositions.Count > 0)
            await messageClearer.Clear(rowBackedPositions);
    }
}
