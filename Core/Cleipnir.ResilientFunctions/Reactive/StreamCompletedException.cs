using System;
using System.Runtime.Serialization;

namespace Cleipnir.ResilientFunctions.Reactive
{
    public class StreamCompletedException : Exception
    {
        public StreamCompletedException() : base("Cannot subscribe to already completed stream") { }
    }
}