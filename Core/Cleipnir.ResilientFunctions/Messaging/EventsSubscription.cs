using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Messaging;

public sealed class EventsSubscription : IAsyncDisposable
{
    private readonly Func<Task<IReadOnlyList<StoredEvent>>> _pullNewEvents;
    private readonly Func<ValueTask> _dispose;
    private bool _disposed;

    private readonly object _sync = new();

    public EventsSubscription(Func<Task<IReadOnlyList<StoredEvent>>> pullNewEvents, Func<ValueTask> dispose)
    {
        _pullNewEvents = pullNewEvents;
        _dispose = dispose;
    }

    public Task<IReadOnlyList<StoredEvent>> PullNewEvents() => _pullNewEvents();

    public ValueTask DisposeAsync()
    {
        lock (_sync)
            if (_disposed) return ValueTask.CompletedTask;
            else _disposed = true;
        
        return _dispose();  
    } 
}