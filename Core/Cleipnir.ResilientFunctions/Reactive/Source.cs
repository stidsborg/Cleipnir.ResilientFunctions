using System;
using System.Collections.Generic;
using System.Linq;

namespace Cleipnir.ResilientFunctions.Reactive;

public interface ISource<T> : IStream<T>
    {
        void Emit(T toEmit);
        void SignalError(Exception exception);
        void SignalCompletion();
    }

public class Source<T> : ISource<T>
{
    private readonly Dictionary<int, Subscription> _subscriptions = new();
    private int _nextSubscriptionId;
    private bool _completed;
    private Exception? _signalledError;

    public IDisposable Subscribe(Action<T> onNext, Action onCompletion, Action<Exception> onError)
    {
        if (_signalledError != null)
        {
            onError(_signalledError);
            return new Subscription(onNext, onCompletion, onError, () => { });
        }

        if (_completed)
        {
            onCompletion();
            return new Subscription(onNext, onCompletion, onError, () => { });
        }

        var subscriptionId = _nextSubscriptionId++;
        var subscription = new Subscription(
            onNext, onCompletion, onError,
            () => _subscriptions.Remove(subscriptionId)
        );

        _subscriptions[subscriptionId] = subscription;
        return subscription;
    }

    public void Emit(T toEmit)
    {
        if (_completed) throw new StreamCompletedException();

        foreach (var subscription in _subscriptions.Values.ToList())
            subscription.OnNext(toEmit);
    }

    public void SignalError(Exception exception)
    {
        if (_completed) throw new StreamCompletedException();
        _completed = true;
        _signalledError = exception;

        foreach (var subscription in _subscriptions.Values.ToList())
            subscription.OnError(exception);
    }

    public void SignalCompletion()
    {
        if (_completed) throw new StreamCompletedException();
        _completed = true;

        foreach (var subscription in _subscriptions.Values.ToList())
            subscription.OnCompletion();
    }

    private class Subscription : IDisposable
    {
        public Action<T> OnNext { get; }
        public Action OnCompletion { get; }
        public Action<Exception> OnError { get; }

        private Action Disposer { get; }

        public Subscription(Action<T> onNext, Action onCompletion, Action<Exception> onError, Action disposer)
        {
            OnNext = onNext;
            OnCompletion = onCompletion;
            OnError = onError;

            Disposer = disposer;
        }

        public void Dispose() => Disposer();
    }
}