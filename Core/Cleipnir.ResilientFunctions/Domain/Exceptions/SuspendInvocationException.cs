using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public class SuspendInvocationException : Exception
{
    public int SuspendUntilEventSourceCountAtLeast { get; }

    public SuspendInvocationException(int suspendUntilEventSourceCountAtLeast)
        => SuspendUntilEventSourceCountAtLeast = suspendUntilEventSourceCountAtLeast;
}