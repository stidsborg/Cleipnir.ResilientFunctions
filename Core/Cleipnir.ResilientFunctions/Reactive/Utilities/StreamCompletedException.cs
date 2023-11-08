using System;

namespace Cleipnir.ResilientFunctions.Reactive.Utilities
{
    public class StreamCompletedException : Exception
    {
        public StreamCompletedException() : base("Cannot subscribe to already completed stream") { }
    }
}