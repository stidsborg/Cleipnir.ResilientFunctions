using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public class FrameworkException : Exception
{
    public FlowType? FlowType { get; }
    public FlowInstance? FlowInstance { get; }

    public FrameworkException(string message, FlowType? flowType = null) : base(message)
    {
        FlowType = flowType;
    }
    
    public FrameworkException(string message, FlowId flowId) : base(message)
    {
        FlowType = flowId.Type;
        FlowInstance = flowId.Instance;
    }
    
    public FrameworkException(string message, Exception innerException, FlowType? flowType = null, FlowInstance? flowInstance = null)
        : base(message, innerException)
    {
        FlowType = flowType;
        FlowInstance = flowInstance;
    }
    
    public FrameworkException(string message, Exception innerException, FlowId flowId)
        : base(message, innerException)
    {
        FlowType = flowId.Type;
        FlowInstance = flowId.Instance;
    }
}