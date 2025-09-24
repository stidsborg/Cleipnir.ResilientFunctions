using System;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class UnexpectedStateException : FrameworkException
{
    public UnexpectedStateException(string message) : base(message) {}
    public UnexpectedStateException(string message, Exception innerException) : base(message, innerException) {}
    public UnexpectedStateException(FlowId flowId, string message)
        : base(message, flowId) {}
    public UnexpectedStateException(FlowId flowId, string message, Exception innerException)
        : base(message, innerException, flowId) {}

    public static UnexpectedStateException NotFound(FlowId flowId) => new(flowId, $"{flowId} was not found");
    public static UnexpectedStateException NotFound(StoredId storedId) => new($"{storedId} was not found");
    public static UnexpectedStateException ConcurrentModification(FlowId flowId) => new(flowId, $"Unable to persist state for '{flowId}' due to concurrent modification");
    public static UnexpectedStateException ConcurrentModification(StoredId storedId) => new($"Unable to persist state for {storedId} due to concurrent modification");
}