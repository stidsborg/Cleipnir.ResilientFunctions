using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;
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

    public ISubscription Subscribe(Action<T> onNext, Action onCompletion, Action<Exception> onError, ISubscriptionGroup? addToSubscriptionGroup = null)
    {
        return new Subscription(
            _inner, 
            _timeoutId,
            _expiresAt,
            _overwriteExisting,
            _signalErrorOnTimeout,
            onNext, 
            onCompletion, 
            onError, 
            addToSubscriptionGroup
        );
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

        public TimeSpan DefaultMessageSyncDelay => _innerSubscription.DefaultMessageSyncDelay;
        public bool DefaultSuspendUntilCompletion => _innerSubscription.DefaultSuspendUntilCompletion;
        
        public Subscription(
            IReactiveChain<T> inner,
            string timeoutId, DateTime expiresAt, bool overwriteExisting, bool signalErrorOnTimeout,
            Action<T> signalNext, Action signalCompletion, Action<Exception> signalError,
            ISubscriptionGroup? subscriptionGroupId)
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
                    _innerSubscription.Group
                );
        }

        public async Task RegisterTimeoutIfNotInExistingEvents()
        {
            var timeoutExists = _innerSubscription
                .Source
                .OfType<TimeoutEvent>()
                .Where(t => t.TimeoutId == _timeoutId)
                .Take(1)
                .Existing(out _)
                .Any();

            if (timeoutExists)
                return;

            try
            {
                await _innerSubscription.TimeoutProvider.RegisterTimeout(_timeoutId, _expiresAt, _overwriteExisting);
            }
            catch (Exception exception)
            {
                OnError(exception);
            }
        }

        public bool IsWorkflowRunning => _innerSubscription.IsWorkflowRunning;
        public ISubscriptionGroup Group => _innerSubscription.Group;
        public IReactiveChain<object> Source => _innerSubscription.Source;
        public ITimeoutProvider TimeoutProvider => _innerSubscription.TimeoutProvider;
        
        public async Task Initialize()
        {
            await _innerSubscription.Initialize();
            await RegisterTimeoutIfNotInExistingEvents();
        }

        public Task SyncStore(TimeSpan maxSinceLastSynced) => _innerSubscription.SyncStore(maxSinceLastSynced);

        public InterruptCount PushMessages() => _innerSubscription.PushMessages();

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
    }
}