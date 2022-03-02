using System;
using System.Runtime.Serialization;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public abstract class RFunctionException : Exception
{
    public RFunctionException() { }

    public RFunctionException(SerializationInfo info, StreamingContext context) : base(info, context) { }

    public RFunctionException(string? message) : base(message) { }

    public RFunctionException(string? message, Exception? innerException) : base(message, innerException) { }
}