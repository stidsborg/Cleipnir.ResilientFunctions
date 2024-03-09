using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public class SuspendInvocationException : Exception
{
    public InterruptCount ExpectedInterruptCount { get; }

    public SuspendInvocationException(InterruptCount expectedInterruptCount)
        => ExpectedInterruptCount = expectedInterruptCount;
}