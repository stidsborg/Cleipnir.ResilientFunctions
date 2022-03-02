using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class InnerFunctionUnhandledException : RFunctionException
{
    public FunctionId FunctionId { get; }

    public InnerFunctionUnhandledException(FunctionId functionId, string message)
        : base(functionId.TypeId, message) => FunctionId = functionId;

    public InnerFunctionUnhandledException(FunctionId functionId, string message, Exception innerException)
        : base(functionId.TypeId, message, innerException) => FunctionId = functionId;
}