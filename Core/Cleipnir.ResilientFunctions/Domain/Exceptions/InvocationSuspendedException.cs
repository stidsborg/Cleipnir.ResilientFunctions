namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class InvocationSuspendedException(FlowId flowId) 
    : FlowTypeException(flowId.Type, $"{flowId} invocation has been suspended")
{
    public FlowId FlowId { get; } = flowId;
}