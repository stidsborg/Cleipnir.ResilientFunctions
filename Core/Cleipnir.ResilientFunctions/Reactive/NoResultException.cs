using System;

namespace Cleipnir.ResilientFunctions.Reactive
{
    public class NoResultException : Exception
    {
        public static NoResultException NewInstance => new();
        private NoResultException() : base(message: "No event was emitted before the stream completed") { }
    }
}