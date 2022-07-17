namespace Cleipnir.ResilientFunctions.Messaging.Core;

public class EventProcessingException : Exception
{
    public EventProcessingException(Exception innerException) 
        : base("Current or previous processing of events failed", innerException) { }
}