using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;

public class SuspendInvocationException(InterruptCount expectedInterruptCount) : Exception
{
    public InterruptCount ExpectedInterruptCount { get; } = expectedInterruptCount;
}