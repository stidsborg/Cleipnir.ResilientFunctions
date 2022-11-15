using System;

namespace Cleipnir.ResilientFunctions.Messaging;

public class EventProcessingException : Exception
{
    public EventProcessingException(Exception innerException) 
        : base("Current or previous processing of events failed", innerException) { }
}