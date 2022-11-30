using System;

namespace Cleipnir.ResilientFunctions.Reactive.Awaiter
{
    public class NoResultException : Exception
    {
        public NoResultException() { }
        public NoResultException(string? message) : base(message) { }
    }
}