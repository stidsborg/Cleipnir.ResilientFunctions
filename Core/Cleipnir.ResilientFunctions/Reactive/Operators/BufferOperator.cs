using System;
using System.Collections.Generic;
using Cleipnir.ResilientFunctions.CoreRuntime;

namespace Cleipnir.ResilientFunctions.Reactive.Operators;

public class BufferOperator<T> : IStream<List<T>>
{
    private readonly IStream<T> _innerStream;
    private readonly int _bufferSize;

    public BufferOperator(IStream<T> innerStream, int bufferSize)
    {
        _innerStream = innerStream;
        _bufferSize = bufferSize;

        if (bufferSize < 0)
            throw new ArgumentException("Buffer size must be a non-negative number", nameof(bufferSize));
    }

    public ISubscription Subscribe(Action<List<T>> onNext, Action onCompletion, Action<Exception> onError, int? subscriptionGroupId = null) 
        => new Subscription(_innerStream, _bufferSize, onNext, onCompletion, onError, subscriptionGroupId);

    private class Subscription : ISubscription
    {
        private readonly Action<List<T>> _signalNext;
        private readonly Action _signalCompletion;
        private readonly Action<Exception> _signalError;

        private readonly int _bufferSize;
        private List<T> _currentBuffer;
        
        private readonly ISubscription _subscription;
        private bool _completed;

        public Subscription(
            IStream<T> inner,
            int bufferSize,
            Action<List<T>> signalNext, Action signalCompletion, Action<Exception> signalError,
            int? subscriptionGroupId)
        {
            _bufferSize = bufferSize;
            _currentBuffer = new List<T>(_bufferSize);
            
            _signalNext = signalNext;
            _signalCompletion = signalCompletion;
            _signalError = signalError;
            
            _subscription = inner.Subscribe(OnNext, OnCompletion, OnError, subscriptionGroupId);
        }

        public IStream<object> Source => _subscription.Source;
        public ITimeoutProvider TimeoutProvider => _subscription.TimeoutProvider;
        public int SubscriptionGroupId => _subscription.SubscriptionGroupId;
        public void DeliverExistingAndFuture() => _subscription.DeliverExistingAndFuture();
        public int DeliverExisting() => _subscription.DeliverExisting();

        private void OnNext(T next)
        {
            if (_completed) return;
            
            _currentBuffer.Add(next);
            if (_currentBuffer.Count != _bufferSize) return;
                
            var buffer = _currentBuffer;
            _currentBuffer = new List<T>(_bufferSize);
            _signalNext(buffer);
        }

        private void OnError(Exception exception)
        {
            if (_completed) return;
            _completed = true;
            _signalError(exception);  
        }

        private void OnCompletion()
        {
            if (_completed) return;
            _completed = true;
            if (_currentBuffer.Count > 0)
                _signalNext(_currentBuffer);
            _signalCompletion();  
        } 

        public void Dispose() => _subscription.Dispose();
    }
}