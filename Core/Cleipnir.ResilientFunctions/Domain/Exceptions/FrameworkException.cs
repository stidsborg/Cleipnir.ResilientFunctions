using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class FrameworkException : RFunctionException
{
    public FrameworkException(FunctionTypeId functionTypeId, string message) 
        : base(functionTypeId, message) {}

    public FrameworkException(FunctionTypeId functionTypeId, string message, Exception innerException) 
        : base(functionTypeId, message, innerException) {}
}