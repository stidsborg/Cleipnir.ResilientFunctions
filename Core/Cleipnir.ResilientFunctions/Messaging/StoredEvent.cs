using System;
using System.Text.Json;

namespace Cleipnir.ResilientFunctions.Messaging;

public record StoredEvent(string EventJson, string EventType, string? IdempotencyKey = null)
{
    public object DefaultDeserialize() => JsonSerializer.Deserialize(EventJson, Type.GetType(EventType, throwOnError: true)!)!;
}