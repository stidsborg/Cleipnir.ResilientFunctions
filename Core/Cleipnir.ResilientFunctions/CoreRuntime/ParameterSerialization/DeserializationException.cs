using System;

namespace Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;

public class DeserializationException : Exception
{
    public DeserializationException(string? message, Exception? innerException) 
        : base(message, innerException) { }
}