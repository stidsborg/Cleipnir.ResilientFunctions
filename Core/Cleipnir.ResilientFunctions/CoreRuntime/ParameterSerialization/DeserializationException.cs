using System;

namespace Cleipnir.ResilientFunctions.ParameterSerialization;

public class DeserializationException : Exception
{
    public DeserializationException(string? message, Exception? innerException) 
        : base(message, innerException) { }
}