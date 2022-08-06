using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions;

public class PostponeInvocationException : Exception
{
    public DateTime PostponeUntil { get; }

    public PostponeInvocationException(TimeSpan postponeFor)
        => PostponeUntil = DateTime.UtcNow.Add(postponeFor);
    public PostponeInvocationException(DateTime postponeUntil) 
        => PostponeUntil = postponeUntil.ToUniversalTime();
}