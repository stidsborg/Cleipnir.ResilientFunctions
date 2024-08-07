using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public abstract class FlowTypeException : Exception
{
    public FlowType FlowType { get; }
    
    public FlowTypeException(FlowType flowType, string message) 
        : base(message) => FlowType = flowType;

    public FlowTypeException(FlowType flowType, string message, Exception innerException) 
        : base(message, innerException) => FlowType = flowType;
}