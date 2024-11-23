using System;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Messaging;

public record StoredMessage(byte[] MessageContent, byte[] MessageType, string? IdempotencyKey = null)
{
    public object DefaultDeserialize() => JsonSerializer.Deserialize(MessageContent, Type.GetType(MessageType.ToStringFromUtf8Bytes(), throwOnError: true)!)!; //todo remove
}