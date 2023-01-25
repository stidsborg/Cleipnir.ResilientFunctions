using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Testing;

public class SpecificFunctionTestHelper : IDisposable
{
    private readonly Utilities _utilities;
    public FunctionId FunctionId { get; }
    public InMemoryFunctionStore FunctionStore { get; }

    private bool Disposed
    {
        get
        {
            lock (_sync)
                return _disposed;
        }
    }
    private bool _disposed;
    private readonly List<IDisposable> _disposables = new();
    private readonly object _sync = new();
    
    public SpecificFunctionTestHelper(FunctionId functionId, InMemoryFunctionStore functionStore, Utilities utilities)
    {
        _utilities = utilities;
        FunctionId = functionId;
        FunctionStore = functionStore;
    }

    public Context Context => CreateContext(InvocationMode.Direct);
    public Context CreateContext(InvocationMode invocationMode)
        => new Context(
            FunctionId,
            invocationMode,
            eventSourceFactory: () => InMemoryEventSource.ToTask(),
            _utilities
        );

    public EventSource InMemoryEventSource
    {
        get
        {
            var es = new EventSource(
                FunctionId,
                FunctionStore.EventStore,
                EventSourceWriter,
                TimeoutProvider,
                pullFrequency: TimeSpan.FromMilliseconds(100),
                DefaultSerializer.Instance
            );
            
            lock (_sync)
            {
                ThrowIfDisposed();
                _disposables.Add(es);    
            }
            
            es.Initialize().Wait();
            return es;
        }
    }

    public EventSourceWriter EventSourceWriter
    {
        get
        {
            ThrowIfDisposed();
            return new EventSourceWriter(
                FunctionId,
                FunctionStore,
                DefaultSerializer.Instance,
                scheduleReInvocation: (id, epoch) => Task.CompletedTask
            );
        }
    }


    public TimeoutProvider TimeoutProvider
    {
        get
        {
            ThrowIfDisposed();
            return new TimeoutProvider(
                FunctionId,
                FunctionStore.TimeoutStore,
                EventSourceWriter,
                timeoutCheckFrequency: TimeSpan.FromMilliseconds(100)
            );            
        }
    }

    public void Dispose()
    {
        lock (_sync)
        {
            if (_disposed) return;
            _disposed = true;
        }
        
        foreach (var disposable in _disposables)
            disposable.Dispose();
    }

    private void ThrowIfDisposed()
    {
        if (!Disposed) return;
        
        throw new ObjectDisposedException(nameof(SpecificFunctionTestHelper));
    }
}