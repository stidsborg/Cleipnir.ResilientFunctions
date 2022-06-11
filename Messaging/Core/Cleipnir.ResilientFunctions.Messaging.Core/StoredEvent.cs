using System.Text.Json;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public record StoredEvent(string EventJson, string EventType, string? IdempotencyKey = null)
{
    public object Deserialize() => JsonSerializer.Deserialize(EventJson, Type.GetType(EventType, throwOnError: true)!)!;
}