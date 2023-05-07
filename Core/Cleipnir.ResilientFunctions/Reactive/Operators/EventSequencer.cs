using System;
using System.Collections.Generic;

namespace Cleipnir.ResilientFunctions.Reactive.Operators;

public class EventSequencer<T>
{
    private readonly Queue<StreamEvent<T>> _eventQueue = new();
    private bool _processing;
    private readonly object _sync = new();
    
    private readonly Action<T> _onNext;
    private readonly Action _onCompletion;
    private readonly Action<Exception> _onError;

    public EventSequencer(Action<T> onNext, Action onCompletion, Action<Exception> onError)
    {
        _onNext = onNext;
        _onCompletion = onCompletion;
        _onError = onError;
    }

    public void HandleNext(T next) => ProcessNewStreamEvents(StreamEvent<T>.CreateFromNext(next));
    public void HandleCompletion() => ProcessNewStreamEvents(StreamEvent<T>.CreateFromCompletion());
    public void HandleError(Exception exception) => StreamEvent<T>.CreateFromException(exception);

    private void ProcessNewStreamEvents(StreamEvent<T> @event)
    {
        StreamEvent<T> dequeuedEvent;
        lock (_sync)
        {
            _eventQueue.Enqueue(@event);
            if (_processing) return;
            _processing = true;
            dequeuedEvent = _eventQueue.Dequeue();
        }

        while (true)
        {
            try
            {
                switch (dequeuedEvent.Status)
                {
                    case StreamEventStatus.SignalNext:
                        _onNext(dequeuedEvent.Next);
                        break;
                    case StreamEventStatus.SignalCompletion:
                        _onCompletion();
                        return;
                    case StreamEventStatus.SignalError:
                        _onError(dequeuedEvent.Error!);
                        return;
                }
            }
            catch (Exception exception)
            {
                try { _onError(exception); } catch (Exception) {}
                return;
            }
            
            lock (_sync)
            {
                if (!_eventQueue.TryDequeue(out dequeuedEvent))
                {
                    _processing = false;
                    return;
                }
            }
        }
    }
}