using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class InnerFunctionUnhandledException : RFunctionException
{
    public FunctionId FunctionId { get; }

    public InnerFunctionUnhandledException(FunctionId functionId, string? message)
        : base(message) => FunctionId = functionId;

    public InnerFunctionUnhandledException(FunctionId functionId, string? message, Exception? innerException)
        : base(message, innerException) => FunctionId = functionId;
}