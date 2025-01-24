using System;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public class DeserializationException : Exception
{
    public DeserializationException(string? message, Exception? innerException) 
        : base(message, innerException) { }
}