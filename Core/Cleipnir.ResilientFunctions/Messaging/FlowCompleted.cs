using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging;

public record FlowCompleted(FlowId Id, byte[]? Result, bool Failed);