using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Queuing;

/// <summary>
/// Deserializes incoming messages at the delivery-pipeline boundary, before they reach the
/// <see cref="QueueManager"/>. A message that fails to deserialize never enters the pipeline: the failure is
/// reported to the unhandled-exception handler and the message is moved to the dead letter queue - appended to
/// the dlq store and deleted from the message store - so it can neither poison the flow nor be endlessly
/// re-fetched. The move is at-least-once (dlq append before row delete), so a crash in between dead letters the
/// message a second time rather than losing it.
/// </summary>
internal class MessageDeserializer(
    FlowId flowId,
    StoredId storedId,
    ISerializer serializer,
    IDlqStore dlqStore,
    IMessageClearer messageClearer,
    UnhandledExceptionHandler unhandledExceptionHandler)
{
    public async Task<DeserializedMessages> Deserialize(IReadOnlyList<IncomingMessage> messages)
    {
        var deserialized = new List<DeserializedMessage>(messages.Count);
        List<IncomingMessage>? deadLettered = null;

        foreach (var message in messages)
        {
            try
            {
                var payload = serializer.Deserialize(message.MessageContent, serializer.ResolveType(message.MessageType)!);
                deserialized.Add(
                    new DeserializedMessage(payload, message.Position, message.IdempotencyKey, message.Sender, message.Receiver)
                );
            }
            catch (Exception exception)
            {
                unhandledExceptionHandler.Invoke(flowId.Type, exception);
                (deadLettered ??= new List<IncomingMessage>()).Add(message);
            }
        }

        if (deadLettered is not null)
            await MoveToDlq(deadLettered);

        return new DeserializedMessages(deserialized, deadLettered ?? []);
    }

    /// <summary>
    /// Deserializes a single message, dead lettering it on failure exactly like <see cref="Deserialize"/> does.
    /// Returns the deserialized payload, or null when the message was dead lettered - the caller must then clear
    /// the message's in-flow carrier (its staged-message child effect).
    /// </summary>
    public async Task<object?> DeserializeOrDeadLetter(IncomingMessage message)
    {
        try
        {
            return serializer.Deserialize(message.MessageContent, serializer.ResolveType(message.MessageType)!);
        }
        catch (Exception exception)
        {
            unhandledExceptionHandler.Invoke(flowId.Type, exception);
            await MoveToDlq([message]);
            return null;
        }
    }

    private async Task MoveToDlq(List<IncomingMessage> messages)
    {
        await dlqStore.Append(
            messages.Select(message => message.ToStoredMessage(storedId)).ToList()
        );

        // Row deletes come after the dlq append has landed - a crash in between re-fetches and re-dead-letters
        // the message rather than losing it. Row-less messages (control-panel appended or completed-flow inlined)
        // have no row to delete; their in-flow carrier is pruned by the caller instead.
        var rowBackedPositions = messages
            .Where(message => message.Position is not null)
            .Select(message => message.Position!.Value)
            .ToList();
        if (rowBackedPositions.Count > 0)
            await messageClearer.Clear(rowBackedPositions);
    }
}

internal record DeserializedMessages(
    IReadOnlyList<DeserializedMessage> Messages,
    IReadOnlyList<IncomingMessage> DeadLettered
);
