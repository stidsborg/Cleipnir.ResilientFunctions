using System;
using System.Runtime.Serialization;

namespace Cleipnir.ResilientFunctions.Domain;

public abstract class RFunctionException : Exception
{
    public RFunctionException() { }

    public RFunctionException(SerializationInfo info, StreamingContext context) : base(info, context) { }

    public RFunctionException(string? message) : base(message) { }

    public RFunctionException(string? message, Exception? innerException) : base(message, innerException) { }
}
    
public sealed class FunctionInvocationException : RFunctionException
{
    public FunctionId FunctionId { get; }

    public FunctionInvocationException(FunctionId functionId, string? message) : base(message) 
        => FunctionId = functionId;

    public FunctionInvocationException(FunctionId functionId, string? message, Exception? innerException) 
        : base(message, innerException) => FunctionId = functionId;
}

public sealed class PreviousFunctionInvocationException : RFunctionException
{
    public FunctionId FunctionId { get; }
    public RError Error { get; }

    public PreviousFunctionInvocationException(FunctionId functionId, RError error, string? message) : base(message)
    {
        FunctionId = functionId;
        Error = error;
    }
}

public sealed class PostponedFunctionInvocationException : RFunctionException
{
    public FunctionId FunctionId { get; }

    public PostponedFunctionInvocationException(FunctionId functionId, string? message) : base(message) 
        => FunctionId = functionId;
}

public sealed class InnerFunctionUnhandledException : RFunctionException
{
    public FunctionId FunctionId { get; }

    public InnerFunctionUnhandledException(FunctionId functionId, string? message)
        : base(message) => FunctionId = functionId;

    public InnerFunctionUnhandledException(FunctionId functionId, string? message, Exception? innerException)
        : base(message, innerException) => FunctionId = functionId;
}

public sealed class FrameworkException : RFunctionException
{
    public FrameworkException() { }

    public FrameworkException(SerializationInfo info, StreamingContext context) : base(info, context) { }

    public FrameworkException(string? message) : base(message) { }

    public FrameworkException(string? message, Exception? innerException) : base(message, innerException) { }
}
    
public sealed class InvalidConfigurationException : Exception //todo consider how to best throw this exception - is a frequency of 1ms ok?
{
    public InvalidConfigurationException(string? message) : base(message) { }
}