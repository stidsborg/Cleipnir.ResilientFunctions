namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class PreviousInvocationException : RFunctionException
{
    public FlowId FlowId { get; }
    public PreviouslyThrownException Exception { get; }

    public PreviousInvocationException(FlowId flowId, PreviouslyThrownException exception) 
        : base(flowId.Type, $"'{flowId}' invocation previously failed")
    {
        FlowId = flowId;
        Exception = exception;
    }
}