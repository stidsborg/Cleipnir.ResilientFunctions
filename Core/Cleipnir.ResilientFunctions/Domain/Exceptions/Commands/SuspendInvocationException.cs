using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;

public class SuspendInvocationException : Exception
{
    public SuspendInvocationException() {}
    public SuspendInvocationException(SuspendInvocationException innerException) 
        : base(message: null, innerException) { }
}