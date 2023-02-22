using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging;

public sealed class EventsSubscription : IAsyncDisposable
{
    private readonly FunctionId _functionId;
    private readonly Action<IEnumerable<StoredEvent>> _callback;
    private readonly Func<ValueTask> _dispose;
    private bool _disposed;
    private readonly UnhandledExceptionHandler? _unhandledExceptionHandler;

    private List<IEnumerable<StoredEvent>> _toDeliver = new(capacity: 2);
    private bool _delivering;
    private readonly object _sync = new();

    public EventsSubscription(
        FunctionId functionId,
        Action<IEnumerable<StoredEvent>> callback, 
        Func<ValueTask> dispose, 
        UnhandledExceptionHandler? unhandledExceptionHandler)
    {
        _functionId = functionId;
        
        _callback = callback;
        _dispose = dispose;
        _unhandledExceptionHandler = unhandledExceptionHandler;
    }

    public void DeliverNewEvents(IEnumerable<StoredEvent> events)
    {
        List<StoredEvent> toDeliver;
        lock (_sync)
        {
            _toDeliver.Add(events);
            if (_delivering || _disposed) return;
            
            _delivering = true;
            toDeliver = _toDeliver.SelectMany(_ => _).ToList();
            _toDeliver = new List<IEnumerable<StoredEvent>>();
        }

        do
        {
            _callback(toDeliver);

            lock (_sync)
            {
                if (_toDeliver.Count == 0 || _disposed)
                {
                    _delivering = false;
                    return;
                }
                
                toDeliver = _toDeliver.SelectMany(_ => _).ToList();
                _toDeliver = new List<IEnumerable<StoredEvent>>();
            }
        } while (true);
    }

    public ValueTask DisposeAsync()
    {
        lock (_sync)
            if (_disposed) return ValueTask.CompletedTask;
            else _disposed = true;
        
        return _dispose();  
    } 
}