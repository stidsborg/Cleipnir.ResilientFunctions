namespace Cleipnir.ResilientFunctions.Messaging;

public record EventAndIdempotencyKey(object Event, string? IdempotencyKey = null);