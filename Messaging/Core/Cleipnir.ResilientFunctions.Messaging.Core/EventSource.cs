using System.Collections.Immutable;
using System.Reactive.Subjects;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging.Core.Serialization;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public class EventSource : IDisposable
{
    private readonly FunctionId _functionId;
    private readonly IEventStore _eventStore;
    private readonly TimeSpan _pullFrequency;
    private readonly IEventSerializer _eventSerializer;

    private readonly HashSet<string> _idempotencyKeys = new();
    
    private int _count;
    private bool _syncingEvents;
    private List<TaskCompletionSource> _tcsQueue = new();
    private readonly object _sync = new();
    
    private volatile bool _disposed;

    private ImmutableList<object> _existing = ImmutableList<object>.Empty;
    public IReadOnlyList<object> Existing
    {
        get
        {
            lock (_sync)
                return _existing;
        }
    }
    
    private readonly ReplaySubject<object> _allSubject = new();
    public IObservable<object> All => _allSubject;
    
    public EventSource(
        FunctionId functionId, 
        IEventStore eventStore, 
        TimeSpan? pullFrequency, 
        IEventSerializer? eventSerializer)
    {
        _functionId = functionId;
        _eventStore = eventStore;
        _eventSerializer = eventSerializer ?? DefaultEventSerializer.Instance;
        _pullFrequency = pullFrequency ?? TimeSpan.FromMilliseconds(250);
    }

    public async Task Initialize()
    {
        await DeliverOutstandingEvents();
        _ = Task.Run(async () =>
        {
            while (!_disposed)
            {
                await Task.Delay(_pullFrequency);
                if (_disposed) return;
                await DeliverOutstandingEvents();
            }
        });
    }

    private Task DeliverOutstandingEvents()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(EventSource));
        
        var tcsToReturn = new TaskCompletionSource();
        
        lock (_sync)
        {
            _tcsQueue.Add(tcsToReturn);
            if (_syncingEvents)
                return tcsToReturn.Task;

            _syncingEvents = true;
        }
        
        //start worker
        Task.Run(async () =>
        {
            while (true)
            {
                List<TaskCompletionSource> enqueued;
                lock (_sync)
                {
                    if (_tcsQueue.Count == 0)
                    {
                        _syncingEvents = false;
                        return;
                    }

                    enqueued = _tcsQueue;
                    _tcsQueue = new List<TaskCompletionSource>();
                }

                try
                {
                    var storedEvents = await _eventStore.GetEvents(_functionId, skip: _count);
                    foreach (var storedEvent in storedEvents)
                    {
                        _count++;

                        if (storedEvent.IdempotencyKey != null)
                            if (_idempotencyKeys.Contains(storedEvent.IdempotencyKey))
                                continue;
                            else
                                _idempotencyKeys.Add(storedEvent.IdempotencyKey);

                        var deserialized = _eventSerializer
                            .DeserializeEvent(storedEvent.EventJson, storedEvent.EventType);
                        lock (_sync)
                            _existing = _existing.Add(deserialized);
                        _allSubject.OnNext(deserialized);
                    }

                    foreach (var enqueuedTcs in enqueued)
                        enqueuedTcs.SetResult();
                }
                catch (Exception e)
                {
                    foreach (var enqueuedTcs in enqueued)
                        enqueuedTcs.TrySetException(e);
                }
            }
        });

        return tcsToReturn.Task;
    }

    public async Task Emit(object @event, string? idempotencyKey = null)
    {
        var json = _eventSerializer.SerializeEvent(@event);
        var type = @event.GetType().SimpleQualifiedName();
        await _eventStore.AppendEvent(_functionId, json, type, idempotencyKey);
        await DeliverOutstandingEvents();
    }

    public Task Pull() => DeliverOutstandingEvents();

    public void Dispose() => _disposed = true;
}