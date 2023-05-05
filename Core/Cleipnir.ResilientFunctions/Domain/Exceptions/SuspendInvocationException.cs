using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public class SuspendInvocationException : Exception
{
    public int ExpectedEventCount { get; }

    public SuspendInvocationException(int expectedEventCount)
        => ExpectedEventCount = expectedEventCount;
}