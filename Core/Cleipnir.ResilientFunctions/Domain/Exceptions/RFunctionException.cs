using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public abstract class RFunctionException : Exception
{
    public FlowType FlowType { get; }
    
    public RFunctionException(FlowType flowType, string message) 
        : base(message) => FlowType = flowType;

    public RFunctionException(FlowType flowType, string message, Exception innerException) 
        : base(message, innerException) => FlowType = flowType;
}