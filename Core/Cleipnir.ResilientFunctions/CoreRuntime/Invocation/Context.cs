using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class Context : IDisposable
{
    public FunctionId FunctionId { get; }
    public EventSource EventSource { get; }
    public Activity Activity { get; }
    public Utilities Utilities { get; }
    
    public Context(FunctionId functionId, EventSource eventSource, Activity activity, Utilities utilities)
    {
        FunctionId = functionId;
        Utilities = utilities;
        EventSource = eventSource;
        Activity = activity;
    }

    public void Deconstruct(out Activity activity, out EventSource eventSource)
    {
        activity = Activity;
        eventSource = EventSource;
    }
    
    public void Dispose() => EventSource.Dispose();
}