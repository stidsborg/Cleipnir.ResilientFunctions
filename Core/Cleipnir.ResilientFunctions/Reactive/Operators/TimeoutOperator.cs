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

    public TimeoutOperator(IReactiveChain<T> inner, string timeoutId, DateTime expiresAt, bool overwriteExisting)
    {
        _inner = inner;
        _timeoutId = timeoutId;
        _expiresAt = expiresAt;
        _overwriteExisting = overwriteExisting;
    }

    public ISubscription Subscribe(Action<T> onNext, Action onCompletion, Action<Exception> onError)
    {
        return new Subscription(
            _inner, 
            _timeoutId,
            _expiresAt,
            _overwriteExisting,
            onNext, 
            onCompletion, 
            onError
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
        private bool _completed;

        public TimeSpan DefaultMessageSyncDelay => _innerSubscription.DefaultMessageSyncDelay;
        public TimeSpan DefaultMessageMaxWait => _innerSubscription.DefaultMessageMaxWait;
        
        public Subscription(
            IReactiveChain<T> inner,
            string timeoutId, DateTime expiresAt, bool overwriteExisting,
            Action<T> signalNext, Action signalCompletion, Action<Exception> signalError)
        {
            _timeoutId = timeoutId;
            _expiresAt = expiresAt;
            _overwriteExisting = overwriteExisting;
            _signalNext = signalNext;
            _signalCompletion = signalCompletion;
            _signalError = signalError;
            
            _innerSubscription = inner.Subscribe(OnNext, OnCompletion, OnError);
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
                OnCompletion();
            
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