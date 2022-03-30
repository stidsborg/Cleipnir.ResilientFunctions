using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class UnexpectedFunctionState : RFunctionException
{
    public FunctionId FunctionId { get; }

    public UnexpectedFunctionState(FunctionId functionId, string message)
        : base(functionId.TypeId, message) => FunctionId = functionId;

    public UnexpectedFunctionState(FunctionId functionId, string message, Exception innerException)
        : base(functionId.TypeId, message, innerException) => FunctionId = functionId;
}