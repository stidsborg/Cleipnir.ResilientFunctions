using System;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public class DeserializationException(string? message, Exception? innerException) : Exception(message, innerException);