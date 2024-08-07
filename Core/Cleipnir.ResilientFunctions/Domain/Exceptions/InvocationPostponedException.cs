using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class InvocationPostponedException(FlowId flowId, DateTime postponedUntil) 
    : FlowTypeException(flowId.Type, $"{flowId} has been postponed until: '{postponedUntil:O}'") 
{
    public FlowId FlowId { get; } = flowId;
    public DateTime PostponedUntil { get; } = postponedUntil;
}