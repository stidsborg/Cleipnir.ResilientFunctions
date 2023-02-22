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
        IEnumerable<StoredEvent> toDeliver;
        lock (_sync)
        {
            if (_disposed) return;
            
            if (_delivering)
            {
                _toDeliver.Add(events);
                return;
            }

            if (_toDeliver.Count > 0)
            {
                toDeliver = _toDeliver.Append(events).SelectMany(_ => _).ToList();
                _toDeliver = new List<IEnumerable<StoredEvent>>(capacity: 2);
            }
            else
                toDeliver = events;
            
            _delivering = true;
        }

        try
        {
            _callback(toDeliver);
        }
        catch (Exception exception) //use UnhandledExceptionHandler
        {
            _unhandledExceptionHandler?.Invoke(_functionId.TypeId, exception);
            DisposeAsync();
            
            return;
        }
        

        while (true)
        {
            lock (_sync)
            {
                if (_disposed) return;
                
                if (_toDeliver.Count > 0)
                {
                    toDeliver = _toDeliver.SelectMany(_ => _).ToList();
                    _toDeliver = new List<IEnumerable<StoredEvent>>(capacity: 2);
                }
                else
                {
                    _delivering = false;
                    return;
                }
            }
            
            try
            {
                _callback(toDeliver);
            }
            catch (Exception exception) //use UnhandledExceptionHandler
            {
                _unhandledExceptionHandler?.Invoke(_functionId.TypeId, exception);
                DisposeAsync();
            
                return;
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (_sync)
            if (_disposed) return ValueTask.CompletedTask;
            else _disposed = true;
        
        return _dispose();  
    } 
}