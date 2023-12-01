using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class Context : IDisposable
{
    public FunctionId FunctionId { get; }
    public EventSource EventSource { get; }
    public Utilities Utilities { get; }
    
    public Context(FunctionId functionId, EventSource eventSource, Utilities utilities)
    {
        FunctionId = functionId;
        Utilities = utilities;
        EventSource = eventSource;
    }

    public void Dispose() => EventSource.Dispose();
}