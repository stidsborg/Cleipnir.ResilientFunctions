namespace Cleipnir.ResilientFunctions.Messaging;

public record MessageAndIdempotencyKey(object Message, string? IdempotencyKey = null);

public static class MessageAndIdempotencyKeyExtensions 
{
    public static MessageAndIdempotencyKey ToMessageAndIdempotencyKey(this object message, string? idempotencyKey = null) 
        => new(message, idempotencyKey);
}