﻿using System;
using Cleipnir.ResilientFunctions.CoreRuntime;

namespace Cleipnir.ResilientFunctions.Reactive.Operators;

public delegate void Operator<in TIn, out TOut>(
    TIn next,
    Action<TOut> notify,
    Action signalCompletion,
    Action<Exception> signalException
);

public delegate void OnCompletion<out TOut>(Action<TOut> notify, Action<Exception> signalException);

public class CustomOperator<TIn, TOut> : IReactiveChain<TOut>
{
    private readonly IReactiveChain<TIn> _inner;
    private readonly Func<Operator<TIn, TOut>> _operatorFactory;
    private readonly OnCompletion<TOut>? _handleCompletion;

    public CustomOperator(
        IReactiveChain<TIn> inner, 
        Func<Operator<TIn, TOut>> operatorFactory,
        OnCompletion<TOut>? handleCompletion)
    {
        _inner = inner;
        _operatorFactory = operatorFactory;
        _handleCompletion = handleCompletion;
    }

    public ISubscription Subscribe(Action<TOut> onNext, Action onCompletion, Action<Exception> onError, int? subscriptionGroupId = null)
        => new Subscription(_inner, _operatorFactory, _handleCompletion, onNext, onCompletion, onError, subscriptionGroupId);

    private class Subscription : ISubscription
    {
        private readonly Action<TOut> _signalNext;
        private readonly Action _signalCompletion;
        private readonly Action<Exception> _signalError;

        private readonly ISubscription _innerSubscription;
        private bool _completed;

        private Operator<TIn, TOut> Operator { get; }
        
        private bool _handlingCompletion;
        private OnCompletion<TOut>? HandleCompletion { get; }

        public IReactiveChain<object> Source => _innerSubscription.Source;

        public Subscription(
            IReactiveChain<TIn> inner,
            Func<Operator<TIn, TOut>> operatorFactory,
            OnCompletion<TOut>? handleCompletion,
            Action<TOut> onNext, Action onCompletion, Action<Exception> onError,
            int? subscriptionGroupId)
        {
            HandleCompletion = handleCompletion;
            _signalNext = onNext;
            _signalCompletion = onCompletion;
            _signalError = onError;

            Operator = operatorFactory();
            
            _innerSubscription = inner.Subscribe(OnNext, OnCompletion, OnError, subscriptionGroupId);
        }

        public ITimeoutProvider TimeoutProvider => _innerSubscription.TimeoutProvider;
        public int SubscriptionGroupId => _innerSubscription.SubscriptionGroupId;
        public void DeliverExistingAndFuture() => _innerSubscription.DeliverExistingAndFuture();
        public int DeliverExisting() => _innerSubscription.DeliverExisting();

        private void OnNext(TIn next)
        {
            if (_completed) return;
            
            try
            {
                Operator(
                    next, 
                    _signalNext, 
                    signalCompletion: () => { OnCompletion(); Dispose(); }, 
                    signalException: exception => { OnError(exception); Dispose(); });
            }
            catch (Exception exception)
            {
                OnError(exception);
            }
        }

        private void OnError(Exception exception)
        {
            _completed = true;
            _signalError(exception);   
        }
        
        private void OnCompletion()
        {
            if (!_completed)
            {
                if (_handlingCompletion) return;
                _handlingCompletion = true;
                
                try
                {
                    HandleCompletion?.Invoke(
                        _signalNext,
                        signalException: exception => { OnError(exception); Dispose(); }
                    );
                }
                catch (Exception exception)
                {
                    OnError(exception);
                }
            }
            
            _completed = true;
            _signalCompletion();
        }

        public void Dispose() => _innerSubscription.Dispose();
    }
}