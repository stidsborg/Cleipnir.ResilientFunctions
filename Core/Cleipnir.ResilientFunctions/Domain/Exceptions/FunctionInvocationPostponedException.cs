using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class FunctionInvocationPostponedException : RFunctionException
{
    public FlowId FlowId { get; }
    public DateTime PostponedUntil { get; }

    public FunctionInvocationPostponedException(FlowId flowId, DateTime postponedUntil)
        : base(
            flowId.Type,
            $"Function '{flowId}' has been postponed until: '{postponedUntil:O}'"
        )
    {
        FlowId = flowId;
        PostponedUntil = postponedUntil;
    } 
}