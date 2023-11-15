using System;

namespace Cleipnir.ResilientFunctions.Messaging;

public class EventProcessingException : Exception
{
    public EventProcessingException(Exception innerException) 
        : base("Processing of events failed", innerException) { }
}