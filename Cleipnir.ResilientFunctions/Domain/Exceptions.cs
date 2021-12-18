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

    public sealed class FrameworkException : RFunctionException
    {
        public FrameworkException() { }

        public FrameworkException(SerializationInfo info, StreamingContext context) : base(info, context) { }

        public FrameworkException(string? message) : base(message) { }

        public FrameworkException(string? message, Exception? innerException) : base(message, innerException) { }
    }
}