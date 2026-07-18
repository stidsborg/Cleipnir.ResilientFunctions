using System;
using System.Collections.Generic;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public record StoredMessage(byte[] MessageContent, byte[] MessageType, long Position, ReplicaId Replica, string? IdempotencyKey = null, string? Sender = null, string? Receiver = null)
{
    /// <summary>
    /// False for messages without a backing message-store row (e.g. appended via the control panel directly
    /// into the flow's effect state). Row-less messages have no store identity: the QueueManager assigns them a
    /// synthetic negative position at staging, and they never participate in row clearing or push dedup.
    /// </summary>
    public bool RowBacked { get; init; } = true;

    public object DefaultDeserialize() => JsonSerializer.Deserialize(MessageContent, Type.GetType(MessageType.ToStringFromUtf8Bytes(), throwOnError: true)!)!; //todo remove

    /// <summary>
    /// An empty message carries no payload - appending one only forces a restart of the receiving flow. It is
    /// never delivered to the flow and is deleted from the store once the restart has happened.
    /// </summary>
    public bool IsEmpty => MessageContent.Length == 0 && MessageType.Length == 0;

    public static StoredMessage CreateEmpty(ReplicaId replica) => new(MessageContent: [], MessageType: [], Position: 0, Replica: replica);
}

public record StoredIdAndMessage(StoredId StoredId, StoredMessage StoredMessage);
public record StoredMessages(StoredId StoredId, List<StoredMessage> Messages);
public static class StoredIdAndMessageExtensions
{
    public static StoredIdAndMessage ToStoredIdAndMessage(this StoredMessage storedMessage, StoredId storedId) 
        => new(storedId, storedMessage);
}