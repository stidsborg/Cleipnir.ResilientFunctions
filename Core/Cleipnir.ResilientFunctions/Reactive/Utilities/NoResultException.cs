using System;

namespace Cleipnir.ResilientFunctions.Reactive.Utilities
{
    public class NoResultException : Exception
    {
        public NoResultException() : base(message: "No event was emitted before the stream completed") { }
    }
}