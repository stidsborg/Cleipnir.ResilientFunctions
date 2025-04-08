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
    private readonly EffectId _timeoutId;
    private readonly DateTime _expiresAt;

    public TimeoutOperator(IReactiveChain<T> inner, EffectId timeoutId, DateTime expiresAt)
    {
        _inner = inner;
        _timeoutId = timeoutId;
        _expiresAt = expiresAt;
    }

    public ISubscription Subscribe(Action<T> onNext, Action onCompletion, Action<Exception> onError)
    {
        return new Subscription(
            _inner, 
            _timeoutId,
            _expiresAt,
            onNext, 
            onCompletion, 
            onError
        );
    }

    private class Subscription : ISubscription
    {
        private readonly EffectId _timeoutId;
        private readonly DateTime _expiresAt;

        private readonly Action<T> _signalNext;
        private readonly Action _signalCompletion;
        private readonly Action<Exception> _signalError;
        
        private readonly ISubscription _innerSubscription;
        private bool _completed;

        public TimeSpan DefaultMessageSyncDelay => _innerSubscription.DefaultMessageSyncDelay;
        public TimeSpan DefaultMessageMaxWait => _innerSubscription.DefaultMessageMaxWait;

        public Subscription(
            IReactiveChain<T> inner,
            EffectId timeoutId, DateTime expiresAt,
            Action<T> signalNext, Action signalCompletion, Action<Exception> signalError)
        {
            _timeoutId = timeoutId;
            _expiresAt = expiresAt;
            _signalNext = signalNext;
            _signalCompletion = signalCompletion;
            _signalError = signalError;
            
            _innerSubscription = inner.Subscribe(OnNext, OnCompletion, OnError);
        }
        
        public bool IsWorkflowRunning => _innerSubscription.IsWorkflowRunning;
        public IReactiveChain<object> Source => _innerSubscription.Source;

        private bool _createdTimeout;

        public Task Initialize() => _innerSubscription.Initialize();

        public async Task SyncStore(TimeSpan maxSinceLastSynced)
        {
            if (!TimeoutExists && !_createdTimeout)
            {
                //append timeout event to messages if it has expired
                var workflow = CurrentFlow.Workflow;
                if (workflow == null)
                    throw new InvalidOperationException("Reactive operator must be invoked by the Cleipnir framework");

                var timeout = await workflow.Effect.CreateOrGet(_timeoutId, _expiresAt);
                if (DateTime.UtcNow >= timeout)
                {
                    var timeoutEvent = new TimeoutEvent(_timeoutId, _expiresAt);
                    var idempotencyKey = $"Timeout¤{_timeoutId}";
                    await workflow.Messages.AppendMessageNoSync(timeoutEvent, idempotencyKey);
                    await _innerSubscription.SyncStore(maxSinceLastSynced: TimeSpan.Zero);
                    return;
                }
            }
            
            await _innerSubscription.SyncStore(maxSinceLastSynced);
        } 

        public void PushMessages() => _innerSubscription.PushMessages();

        private bool TimeoutExists => _innerSubscription
            .Source
            .OfType<TimeoutEvent>()
            .Where(t => t.TimeoutId == _timeoutId)
            .Take(1)
            .Existing(out _)
            .Any();
        
        public async Task<RegisterTimeoutResult?> RegisterTimeout()
        {
            if (TimeoutExists || _createdTimeout)
                return null;
            
            var workflow = CurrentFlow.Workflow;
            if (workflow == null)
                throw new InvalidOperationException("Reactive operator must be invoked by the Cleipnir framework");
            
            var timeout = await workflow.Effect.CreateOrGet(_timeoutId with {Id = _timeoutId.Id + "_Expires"}, _expiresAt, flush: false);
            if (timeout > DateTime.UtcNow) 
                return new RegisterTimeoutResult(timeout, AppendedTimeoutToMessages: false);
            
            //append timeout event to messages
            var timeoutEvent = new TimeoutEvent(_timeoutId, _expiresAt);
            var idempotencyKey = $"Timeout¤{_timeoutId}";
            await workflow.Messages.AppendMessageNoSync(timeoutEvent, idempotencyKey);
            _createdTimeout = true;
            return new RegisterTimeoutResult(TimeoutExpiry: null, AppendedTimeoutToMessages: true);
        }

        public Task CancelTimeout() => Task.CompletedTask;

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