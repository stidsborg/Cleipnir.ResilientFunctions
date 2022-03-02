using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public sealed class FrameworkException : RFunctionException
{
    public FrameworkException(string? message) : base(message) { }

    public FrameworkException(string? message, Exception? innerException) : base(message, innerException) { }
}