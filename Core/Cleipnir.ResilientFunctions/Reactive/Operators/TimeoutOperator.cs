﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace Cleipnir.ResilientFunctions.Reactive.Operators;

public class TimeoutOperator<T> : IReactiveChain<T>
{
    private readonly IReactiveChain<T> _inner;
    private readonly string _timeoutId;
    private readonly DateTime _expiresAt;
    private readonly bool _overwriteExisting;
    private readonly bool _signalErrorOnTimeout;

    public TimeoutOperator(IReactiveChain<T> inner, string timeoutId, DateTime expiresAt, bool overwriteExisting, bool signalErrorOnTimeout)
    {
        _inner = inner;
        _timeoutId = timeoutId;
        _expiresAt = expiresAt;
        _overwriteExisting = overwriteExisting;
        _signalErrorOnTimeout = signalErrorOnTimeout;
    }

    public ISubscription Subscribe(Action<T> onNext, Action onCompletion, Action<Exception> onError, int? subscriptionGroupId = null)
    {
        var subscription = new Subscription(
            _inner, 
            _timeoutId,
            _expiresAt,
            _overwriteExisting,
            _signalErrorOnTimeout,
            onNext, 
            onCompletion, 
            onError, 
            subscriptionGroupId
        );
        
        Task.Run(subscription.RegisterTimeoutIfNotInExistingEvents);

        return subscription;
    }

    private class Subscription : ISubscription
    {
        private readonly string _timeoutId;
        private readonly DateTime _expiresAt;
        private readonly bool _overwriteExisting;

        private readonly Action<T> _signalNext;
        private readonly Action _signalCompletion;
        private readonly Action<Exception> _signalError;
        
        private readonly ISubscription _innerSubscription;
        private readonly ISubscription _timeoutSubscription;
        private bool _completed;

        public Subscription(
            IReactiveChain<T> inner,
            string timeoutId, DateTime expiresAt, bool overwriteExisting, bool signalErrorOnTimeout,
            Action<T> signalNext, Action signalCompletion, Action<Exception> signalError,
            int? subscriptionGroupId)
        {
            _timeoutId = timeoutId;
            _expiresAt = expiresAt;
            _overwriteExisting = overwriteExisting;
            _signalNext = signalNext;
            _signalCompletion = signalCompletion;
            _signalError = signalError;
            
            _innerSubscription = inner.Subscribe(OnNext, OnCompletion, OnError, subscriptionGroupId);

            _timeoutSubscription = _innerSubscription
                .Source
                .OfType<TimeoutEvent>()
                .Where(t => t.TimeoutId == timeoutId)
                .Take(1)
                .Subscribe(
                    onNext: _ => { },
                    onCompletion: () => { if (signalErrorOnTimeout) OnError(new TimeoutException($"Timeout '{timeoutId}' expired")); else OnCompletion(); },
                    onError: _ => { },
                    _innerSubscription.SubscriptionGroupId
                );
        }

        public async Task RegisterTimeoutIfNotInExistingEvents()
        {
            var timeoutExists = _innerSubscription
                .Source
                .OfType<TimeoutEvent>()
                .Where(t => t.TimeoutId == _timeoutId)
                .Take(1)
                .Existing()
                .Any();

            if (timeoutExists)
                return;

            try
            {
                await _innerSubscription.TimeoutProvider.RegisterTimeout(_timeoutId, _expiresAt, _overwriteExisting);
            }
            catch (Exception exception)
            {
                await StopDelivering();
                OnError(exception);
            }
        }

        public int EmittedFromSource => _innerSubscription.EmittedFromSource;
        public IReactiveChain<object> Source => _innerSubscription.Source;
        public ITimeoutProvider TimeoutProvider => _innerSubscription.TimeoutProvider;
        public int SubscriptionGroupId => _innerSubscription.SubscriptionGroupId;
        public void DeliverFuture() => _innerSubscription.DeliverFuture();
        public void DeliverExisting() => _innerSubscription.DeliverExisting();
        public Task StopDelivering() => _innerSubscription.StopDelivering();

        private void OnNext(T next)
        {
            if (next is TimeoutEvent t && t.TimeoutId == _timeoutId)
                return;

            if (_completed) return;
            
            _signalNext(next);
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

            _signalCompletion();  
        }

        public void Dispose()
        {
            _innerSubscription.Dispose();
            _timeoutSubscription.Dispose();
        } 
    }
}