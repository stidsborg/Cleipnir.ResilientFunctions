using System;
using System.Text.Json;

namespace Cleipnir.ResilientFunctions.Messaging;

public record StoredMessage(string MessageJson, string MessageType, string? IdempotencyKey = null)
{
    public object DefaultDeserialize() => JsonSerializer.Deserialize(MessageJson, Type.GetType(MessageType, throwOnError: true)!)!;
}