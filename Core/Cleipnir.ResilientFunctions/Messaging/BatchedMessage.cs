using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging;

public record BatchedMessage(FlowInstance Instance, object Message, string? IdempotencyKey = null);