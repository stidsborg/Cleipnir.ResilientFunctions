using System;
using System.Runtime.Serialization;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class FrameworkException : RFunctionException
{
    public FrameworkException() { }

    public FrameworkException(SerializationInfo info, StreamingContext context) : base(info, context) { }

    public FrameworkException(string? message) : base(message) { }

    public FrameworkException(string? message, Exception? innerException) : base(message, innerException) { }
}