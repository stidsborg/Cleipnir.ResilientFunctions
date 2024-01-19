using System;

namespace Cleipnir.ResilientFunctions.Messaging;

public class MessageProcessingException : Exception
{
    public MessageProcessingException(Exception innerException) 
        : base("Processing of events failed", innerException) { }
}