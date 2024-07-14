using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class UnexpectedFunctionState : RFunctionException
{
    public FlowId FlowId { get; }

    public UnexpectedFunctionState(FlowId flowId, string message)
        : base(flowId.Type, message) => FlowId = flowId;

    public UnexpectedFunctionState(FlowId flowId, string message, Exception innerException)
        : base(flowId.Type, message, innerException) => FlowId = flowId;
}