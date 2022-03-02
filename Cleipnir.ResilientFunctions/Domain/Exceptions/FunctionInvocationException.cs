using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class FunctionInvocationException : RFunctionException
{
    public FunctionId FunctionId { get; }

    public FunctionInvocationException(FunctionId functionId, string message)
        : base(functionId.TypeId, message) => FunctionId = functionId;

    public FunctionInvocationException(FunctionId functionId, string message, Exception innerException)
        : base(functionId.TypeId, message, innerException) => FunctionId = functionId;
}