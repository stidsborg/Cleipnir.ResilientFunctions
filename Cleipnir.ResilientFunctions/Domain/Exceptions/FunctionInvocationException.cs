using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class FunctionInvocationException : RFunctionException
{
    public FunctionId FunctionId { get; }

    public FunctionInvocationException(FunctionId functionId, string? message) : base(message) 
        => FunctionId = functionId;

    public FunctionInvocationException(FunctionId functionId, string? message, Exception? innerException) 
        : base(message, innerException) => FunctionId = functionId;
}