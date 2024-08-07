using System;

namespace Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;

public class PostponeInvocationException : Exception
{
    public DateTime PostponeUntil { get; }

    public PostponeInvocationException(TimeSpan postponeFor)
        => PostponeUntil = DateTime.UtcNow.Add(postponeFor);
    public PostponeInvocationException(int postponeForMs)
        => PostponeUntil = DateTime.UtcNow.AddMilliseconds(postponeForMs);
    public PostponeInvocationException(DateTime postponeUntil) 
        => PostponeUntil = postponeUntil.ToUniversalTime();
}