using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Messaging;

public sealed class EventsSubscription : IAsyncDisposable
{
    private readonly Func<Task<IReadOnlyList<StoredEvent>>> _pullEvents;
    private readonly Func<ValueTask> _dispose;
    private bool _disposed;

    private readonly object _sync = new();

    public EventsSubscription(Func<Task<IReadOnlyList<StoredEvent>>> pullEvents, Func<ValueTask> dispose)
    {
        _pullEvents = pullEvents;
        _dispose = dispose;
    }

    public Task<IReadOnlyList<StoredEvent>> Pull() => _pullEvents();

    public ValueTask DisposeAsync()
    {
        lock (_sync)
            if (_disposed) return ValueTask.CompletedTask;
            else _disposed = true;
        
        return _dispose();  
    } 
}