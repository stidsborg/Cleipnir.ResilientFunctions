using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Reactive;

internal class StreamAsyncEnumerator<T> : IAsyncEnumerator<T>
    {
        private readonly ISubscription _subscription;
        private readonly Dictionary<int, TaskCompletionSource<Event>> _events = new();
        private int _writeIndex = 0;
        private int _readIndex = 0;
        private readonly object _sync = new();

        private Exception? _thrownException;
        private T _current = default!;

        public T Current
        {
            get
            {
                if (_thrownException != null)
                    throw _thrownException;
                
                return _current;
            }
        }

        public StreamAsyncEnumerator(IReactiveChain<T> reactiveChain, CancellationToken cancellationToken)
        {
            _subscription = reactiveChain.Subscribe(
                onNext: next => AddEventToDictionary(new Event(completed: false, exception: null, next)),
                onCompletion: () => AddEventToDictionary(new Event(completed: true, exception: null, next: default!)),
                onError: exception => AddEventToDictionary(new Event(completed: false, exception, next: default!))
            );
            _subscription.DeliverExistingAndFuture();

            cancellationToken.Register(_subscription.Dispose);
        }

        private void AddEventToDictionary(Event @event)
        {
            TaskCompletionSource<Event> tcs;
            lock (_sync)
            {
                tcs = _events.ContainsKey(_writeIndex) 
                    ? _events[_writeIndex] 
                    : new TaskCompletionSource<Event>();
                
                _events[_writeIndex] = tcs;
                _writeIndex++;    
            }
            
            tcs.SetResult(@event);
        }
        
        public ValueTask DisposeAsync()
        {
            _subscription.Dispose();
            return ValueTask.CompletedTask;
        }

        public ValueTask<bool> MoveNextAsync()
        {
            TaskCompletionSource<Event> tcs;
            
            lock (_sync)
            {
                if (!_events.ContainsKey(_readIndex))
                {
                    tcs = new TaskCompletionSource<Event>();
                    _events[_readIndex] = tcs;
                } 
                else
                    tcs = _events[_readIndex];

                _readIndex++;
            }

            var task = tcs.Task.ContinueWith(t =>
            {
                var @event = t.Result;
                if (!@event.Completed && @event.Exception == null)
                    _current = @event.Next;
                else if (@event.Exception != null)
                    _thrownException = @event.Exception;
                    
                return !@event.Completed;
            });
            return new ValueTask<bool>(task);
        }

        private readonly struct Event
        {
            public bool Completed { get; }
            public Exception? Exception { get; }
            public T Next { get; }

            public Event(bool completed, Exception? exception, T next)
            {
                Completed = completed;
                Exception = exception;
                Next = next;
            }
        }
    }