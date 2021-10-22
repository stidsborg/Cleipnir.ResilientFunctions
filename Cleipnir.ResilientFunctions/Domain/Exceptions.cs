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
    
    public sealed class RFunctionInvocationException : RFunctionException
    {
        public RFunctionInvocationException() { }

        public RFunctionInvocationException(SerializationInfo info, StreamingContext context) : base(info, context) { }

        public RFunctionInvocationException(string? message) : base(message) { }

        public RFunctionInvocationException(string? message, Exception? innerException) : base(message, innerException) { }
    }

    public sealed class FrameworkException : RFunctionException
    {
        public FrameworkException() { }

        public FrameworkException(SerializationInfo info, StreamingContext context) : base(info, context) { }

        public FrameworkException(string? message) : base(message) { }

        public FrameworkException(string? message, Exception? innerException) : base(message, innerException) { }
    }
}