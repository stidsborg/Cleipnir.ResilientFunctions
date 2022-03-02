using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public abstract class RFunctionException : Exception
{
    public FunctionTypeId FunctionTypeId { get; }
    
    public RFunctionException(FunctionTypeId functionTypeId, string message) 
        : base(message) => FunctionTypeId = functionTypeId;

    public RFunctionException(FunctionTypeId functionTypeId, string message, Exception innerException) 
        : base(message, innerException) => FunctionTypeId = functionTypeId;
}