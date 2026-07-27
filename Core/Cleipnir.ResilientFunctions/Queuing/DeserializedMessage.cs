namespace Cleipnir.ResilientFunctions.Queuing;

/// <summary>
/// A message whose payload was deserialized at the delivery-pipeline boundary (<see cref="MessageDeserializer"/>)
/// - the QueueManager only ever receives messages that deserialized successfully; the rest are dead lettered
/// before reaching it. Deliberately carries no serialized bytes: the durable carriers (the staged-message child
/// and the delivered-message capture) re-serialize the payload at staging instead.
/// </summary>
internal record DeserializedMessage(
    object Message,
    long? Position,
    string? IdempotencyKey = null,
    string? Sender = null,
    string? Receiver = null);
