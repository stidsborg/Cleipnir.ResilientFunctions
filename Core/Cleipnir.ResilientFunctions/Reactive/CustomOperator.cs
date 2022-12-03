using System;

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

        private readonly ISubscription _subscription;
        private volatile bool _completed;

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
            
            _subscription = inner.Subscribe(SignalNext, SignalCompletion, SignalError);
        }

        public void Start() => _subscription.Start();

        private void SignalNext(TIn next)
        {
            if (_completed) return;

            try
            {
                Operator(next, _onNext, _onCompletion, _onError);
            }
            catch (Exception exception)
            {
                SignalError(exception);
            }
        }

        private void SignalError(Exception exception)
        {
            if (_completed) return;

            Dispose();
            _onError(exception);
        }

        private void SignalCompletion()
        {
            if (_completed) return;

            Dispose();
            _onCompletion();
        }

        public void Dispose()
        {
            if (_completed) return;
            
            _completed = true;
            _subscription.Dispose();
        }
    }
}