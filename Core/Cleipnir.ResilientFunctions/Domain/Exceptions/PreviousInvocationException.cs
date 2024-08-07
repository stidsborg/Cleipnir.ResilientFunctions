namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class PreviousInvocationException(FlowId flowId, PreviouslyThrownException exception)
    : FlowTypeException(flowId.Type, $"'{flowId}' invocation previously failed")
{
    public FlowId FlowId { get; } = flowId;
    public PreviouslyThrownException Exception { get; } = exception;
}