using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;

public class SuspendInvocationException : Exception
{
    public InterruptCount ExpectedInterruptCount { get; }

    public SuspendInvocationException(InterruptCount expectedInterruptCount)
        => ExpectedInterruptCount = expectedInterruptCount;
}