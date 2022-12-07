using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Reactive;

public delegate void Operator<in TIn, out TOut>(
    TIn next,
    Action<TOut> notify,
    Action signalCompletion,
    Action<Exception> signalException
);

public class CustomOperator<TIn, TOut> : IStream<TOut>
{
    private readonly IStream<TIn> _inner;
    private readonly Func<Operator<TIn, TOut>> _operatorFactory;
    public int TotalEventCount => _inner.TotalEventCount;
    
    public CustomOperator(IStream<TIn> inner, Func<Operator<TIn, TOut>> operatorFactory)
    {
        _inner = inner;
        _operatorFactory = operatorFactory;
    }

    public ISubscription Subscribe(Action<TOut> onNext, Action onCompletion, Action<Exception> onError)
        => new Subscription(_inner, _operatorFactory, onNext, onCompletion, onError);

    private class Subscription : ISubscription
    {
        private readonly Action<TOut> _onNext;
        private readonly Action _onCompletion;
        private readonly Action<Exception> _onError;

        private readonly ISubscription _innerSubscription;
        private bool _completed;

        private Operator<TIn, TOut> Operator { get; }

        public Subscription(
            IStream<TIn> inner,
            Func<Operator<TIn, TOut>> operatorFactory,
            Action<TOut> onNext, Action onCompletion, Action<Exception> onError)
        {
            _onNext = onNext;
            _onCompletion = onCompletion;
            _onError = onError;

            Operator = operatorFactory();
            
            _innerSubscription = inner.Subscribe(SignalNext, SignalCompletion, SignalError);
        }

        public int EventSourceTotalCount => _innerSubscription.EventSourceTotalCount;
        public void Start() => _innerSubscription.Start();
        public void ReplayUntil(int count) => _innerSubscription.ReplayUntil(count);

        private void SignalNext(TIn next)
        {
            if (_completed) return;

            try
            {
                Operator(next, _onNext, SignalCompletion, SignalError);
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
            Dispose();
        }

        private void SignalCompletion()
        {
            if (_completed) return;
            _completed = true;
            
            _onCompletion();
            Dispose();
        }

        public void Dispose() => _innerSubscription.Dispose();
    }
}