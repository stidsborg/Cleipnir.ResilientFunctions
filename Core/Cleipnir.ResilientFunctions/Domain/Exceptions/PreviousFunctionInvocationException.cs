namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class PreviousFunctionInvocationException : RFunctionException
{
    public FlowId FlowId { get; }
    public PreviouslyThrownException Exception { get; }

    public PreviousFunctionInvocationException(FlowId flowId, PreviouslyThrownException exception) 
        : base(flowId.Type, $"'{flowId}' function invocation previously failed")
    {
        FlowId = flowId;
        Exception = exception;
    }
}