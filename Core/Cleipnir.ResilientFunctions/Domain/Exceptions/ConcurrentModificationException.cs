using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class ConcurrentModificationException : FlowTypeException
{
    public FlowId FlowId { get; }

    public ConcurrentModificationException(FlowId flowId)
        : base(
            flowId.Type,
            $"Unable to persist state for '{flowId}' due to concurrent modification"
        ) => FlowId = flowId;
    
    public ConcurrentModificationException(FlowId flowId, Exception innerException)
        : base(
            flowId.Type,
            $"Unable to persist state for '{flowId}' due to concurrent modification",
            innerException
        ) => FlowId = flowId;
}