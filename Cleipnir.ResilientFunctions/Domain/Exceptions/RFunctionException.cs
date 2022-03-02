using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public abstract class RFunctionException : Exception
{
    public RFunctionException(string? message) : base(message) { }

    public RFunctionException(string? message, Exception? innerException) : base(message, innerException) { }
}