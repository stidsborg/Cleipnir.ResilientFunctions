using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class Context
{
    public FunctionId FunctionId { get; }

    private readonly Func<Task<EventSource>> _eventSourceFactory;
    private Task<EventSource>? _eventSource;
    private readonly object _sync = new();

    public Task<EventSource> EventSource
    {
        get
        {
            lock (_sync)
                return _eventSource ??= _eventSourceFactory();
        }
    }

    public InvocationMode InvocationMode { get; }
    public Utilities Utilities { get; }
    
    public Context(FunctionId functionId, InvocationMode invocationMode, Func<Task<EventSource>> eventSourceFactory, Utilities utilities)
    {
        FunctionId = functionId;
        InvocationMode = invocationMode;
        _eventSourceFactory = eventSourceFactory;
        Utilities = utilities;
    }
}