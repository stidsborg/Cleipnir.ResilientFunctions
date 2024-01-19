namespace Cleipnir.ResilientFunctions.Messaging;

public record MessageAndIdempotencyKey(object Message, string? IdempotencyKey = null);