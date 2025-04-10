using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;

public class PostponeInvocationException(DateTime postponeUntil) : Exception
{
    public DateTime PostponeUntil { get; } = postponeUntil.ToUniversalTime();
}