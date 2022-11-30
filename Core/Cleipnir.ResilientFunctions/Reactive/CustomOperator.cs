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
    private readonly Func<Operator<TIn, TOut>> _operatorFunc;

    public CustomOperator(IStream<TIn> inner, Func<Operator<TIn, TOut>> operatorFunc)
    {
        _inner = inner;
        _operatorFunc = operatorFunc;
    }

    public IDisposable Subscribe(Action<TOut> onNext, Action onCompletion, Action<Exception> onError)
        => new Subscription(_inner, _operatorFunc, onNext, onCompletion, onError);

    private class Subscription : IDisposable
    {
        private readonly Action<TOut> _onNext;
        private readonly Action _onCompletion;
        private readonly Action<Exception> _onError;

        private IDisposable? _subscription;
        private bool _completed;
        private readonly object _sync = new();

        public Operator<TIn, TOut> Operator { get; }

        public Subscription(
            IStream<TIn> inner,
            Func<Operator<TIn, TOut>> operatorFunc,
            Action<TOut> onNext, Action onCompletion, Action<Exception> onError)
        {
            _onNext = onNext;
            _onCompletion = onCompletion;
            _onError = onError;

            _subscription = inner.Subscribe(SignalNext, SignalCompletion, SignalError);

            Operator = operatorFunc();
        }

        private void SignalNext(TIn next)
        {
            if (_completed)
                return;

            Operator(next, _onNext, _onCompletion, _onError);
        }

        private void SignalError(Exception exception)
        {
            if (_completed)
                return;

            Dispose();
            _onError(exception);
        }

        private void SignalCompletion()
        {
            if (_completed)
                return;

            Dispose();
            _onCompletion();
        }

        public void Dispose()
        {
            lock (_sync)
            {
                if (_completed) return;
                _completed = true;
            }
            
            _subscription?.Dispose();
            _subscription = null;
        }
    }
}