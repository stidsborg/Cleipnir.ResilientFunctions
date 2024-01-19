using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class Context : IDisposable
{
    public FunctionId FunctionId { get; }
    public Messages Messages { get; }
    public Activities Activities { get; }
    public Utilities Utilities { get; }
    
    public Context(FunctionId functionId, Messages messages, Activities activities, Utilities utilities)
    {
        FunctionId = functionId;
        Utilities = utilities;
        Messages = messages;
        Activities = activities;
    }

    public void Deconstruct(out Activities activities, out Messages messages)
    {
        activities = Activities;
        messages = Messages;
    }
    
    public void Dispose() => Messages.Dispose();
}