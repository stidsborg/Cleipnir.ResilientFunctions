using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Messaging;

public sealed class EventsSubscription : IDisposable
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

    public void Dispose()
    {
        lock (_sync)
            if (_disposed) return;
            else _disposed = true;
        
        _dispose();  
    } 
}