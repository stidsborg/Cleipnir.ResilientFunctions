using System;
using System.Runtime.Serialization;

namespace Cleipnir.ResilientFunctions.Domain
{
    public abstract class RFunctionException : Exception
    {
        public RFunctionException() { }

        public RFunctionException(SerializationInfo info, StreamingContext context) : base(info, context) { }

        public RFunctionException(string? message) : base(message) { }

        public RFunctionException(string? message, Exception? innerException) : base(message, innerException) { }
    }
    
    public sealed class FunctionInvocationException : RFunctionException
    {
        public FunctionInvocationException() { }

        public FunctionInvocationException(SerializationInfo info, StreamingContext context) : base(info, context) { }

        public FunctionInvocationException(string? message) : base(message) { }

        public FunctionInvocationException(string? message, Exception? innerException) : base(message, innerException) { }
    }
    
    public sealed class FunctionBarricadedException : RFunctionException
    {
        public FunctionBarricadedException() { }

        public FunctionBarricadedException(SerializationInfo info, StreamingContext context) : base(info, context) { }

        public FunctionBarricadedException(string? message) : base(message) { }

        public FunctionBarricadedException(string? message, Exception? innerException) : base(message, innerException) { }
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
}