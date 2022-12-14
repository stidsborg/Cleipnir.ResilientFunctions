using System;

namespace Cleipnir.ResilientFunctions.Reactive;

public class MergeOperator<T> : IStream<T>
{
    private readonly IStream<T> _stream1;
    private readonly IStream<T> _stream2;
    
    public MergeOperator(IStream<T> stream1, IStream<T> stream2)
    {
        _stream1 = stream1;
        _stream2 = stream2;
    }

    public ISubscription Subscribe(Action<T> onNext, Action onCompletion, Action<Exception> onError, int? subscriptionGroupId = null)
        => new Subscription(_stream1, _stream2, onNext, onCompletion, onError, subscriptionGroupId);

    private class Subscription : ISubscription
    {
        private readonly Action<T> _onNext;
        private readonly Action _onCompletion;
        private readonly Action<Exception> _onError;

        private readonly ISubscription _subscription1;
        private bool _subscription1Completed;
        private readonly ISubscription _subscription2;
        private bool _subscription2Completed;
        private bool _completed;
        
        public Subscription(
            IStream<T> inner1,
            IStream<T> inner2,
            Action<T> onNext, Action onCompletion, Action<Exception> onError,
            int? subscriptionGroupId)
        {
            _onNext = onNext;
            _onCompletion = onCompletion;
            _onError = onError;

            _subscription1 = inner1.Subscribe(SignalNext, SignalCompletion1, SignalError, subscriptionGroupId);
            _subscription2 = inner2.Subscribe(SignalNext, SignalCompletion2, SignalError, _subscription1.SubscriptionGroupId);
            SubscriptionGroupId = _subscription1.SubscriptionGroupId;
        }

        public int SubscriptionGroupId { get; }
        public void DeliverExistingAndFuture() => _subscription1.DeliverExistingAndFuture();
        public int DeliverExisting() => _subscription1.DeliverExisting();

        private void SignalNext(T next)
        {
            if (_completed) return;

            try
            {
                _onNext(next);
            }
            catch (Exception exception)
            {
                SignalError(exception);
            }
        }

        private void SignalError(Exception exception)
        {
            if (_completed) return;
            _completed = true;

            _onError(exception);

            _subscription1Completed = true;
            _subscription1.Dispose();
            _subscription2Completed = true;
            _subscription2.Dispose();
        }

        private void SignalCompletion1()
        {
            if (_subscription1Completed) return;
            _subscription1Completed = true;
            _subscription1.Dispose();
            
            if (!(_subscription1Completed && _subscription2Completed)) return;
            
            _completed = true;

            _onCompletion();
        }
        
        private void SignalCompletion2()
        {
            if (_subscription2Completed) return;
            _subscription2Completed = true;
            _subscription2.Dispose();
            
            if (!(_subscription1Completed && _subscription2Completed)) return;
            
            _completed = true;

            _onCompletion();
        }

        public void Dispose()
        {
            if (!_subscription1Completed)
                _subscription1.Dispose();
            
            _subscription1Completed = true;

            if (!_subscription2Completed)
                _subscription2.Dispose();
            
            _subscription2Completed = true;
        }
    }
}