using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class FunctionInvocationPostponedException : RFunctionException
{
    public FunctionId FunctionId { get; }
    public DateTime PostponedUntil { get; }

    public FunctionInvocationPostponedException(FunctionId functionId, DateTime postponedUntil)
        : base(
            functionId.TypeId,
            $"Function '{functionId}' has been postponed until: '{postponedUntil:O}'"
        )
    {
        FunctionId = functionId;
        PostponedUntil = postponedUntil;
    } 
}