namespace Cleipnir.ResilientFunctions.Messaging;

public record Envelope(object Message, string? Receiver, string? Sender);