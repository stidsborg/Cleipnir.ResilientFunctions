using System;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Messaging;

public record StoredMessage(byte[] MessageContent, byte[] MessageType, long Position, string? IdempotencyKey = null, string? Sender = null, string? Receiver = null, ReplicaId? Replica = null)
{
    public object DefaultDeserialize() => JsonSerializer.Deserialize(MessageContent, Type.GetType(MessageType.ToStringFromUtf8Bytes(), throwOnError: true)!)!; //todo remove
}

public record StoredIdAndMessage(StoredId StoredId, StoredMessage StoredMessage);
public static class StoredIdAndMessageExtensions
{
    public static StoredIdAndMessage ToStoredIdAndMessage(this StoredMessage storedMessage, StoredId storedId) 
        => new(storedId, storedMessage);
}