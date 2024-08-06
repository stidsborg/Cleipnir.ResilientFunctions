using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;

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

    public ISubscription Subscribe(Action<TOut> onNext, Action onCompletion, Action<Exception> onError)
        => new Subscription(_inner, _operatorFactory, _handleCompletion, onNext, onCompletion, onError);

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

        public bool IsWorkflowRunning => _innerSubscription.IsWorkflowRunning;
        public IReactiveChain<object> Source => _innerSubscription.Source;
        public TimeSpan DefaultMessageSyncDelay => _innerSubscription.DefaultMessageSyncDelay;
        public TimeSpan DefaultMessageMaxWait => _innerSubscription.DefaultMessageMaxWait;

        public Subscription(
            IReactiveChain<TIn> inner,
            Func<Operator<TIn, TOut>> operatorFactory,
            OnCompletion<TOut>? handleCompletion,
            Action<TOut> onNext, Action onCompletion, Action<Exception> onError)
        {
            HandleCompletion = handleCompletion;
            _signalNext = onNext;
            _signalCompletion = onCompletion;
            _signalError = onError;

            Operator = operatorFactory();
            
            _innerSubscription = inner.Subscribe(OnNext, OnCompletion, OnError);
        }

        public IRegisteredTimeouts RegisteredTimeouts => _innerSubscription.RegisteredTimeouts;

        public Task Initialize() => _innerSubscription.Initialize();

        public Task SyncStore(TimeSpan maxSinceLastSynced) => _innerSubscription.SyncStore(maxSinceLastSynced);
        public InterruptCount PushMessages() => _innerSubscription.PushMessages();
        
        public Task RegisterTimeout() => _innerSubscription.RegisterTimeout();
        public Task CancelTimeout() => _innerSubscription.CancelTimeout();

        private void OnNext(TIn next)
        {
            if (_completed) return;
            
            try
            {
                Operator(
                    next,
                    _signalNext,
                    signalCompletion: OnCompletion,
                    signalException: OnError
                );
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
            if (_completed) return;
            
            if (_handlingCompletion) return;
            _handlingCompletion = true;
                
            try
            {
                HandleCompletion?.Invoke(
                    _signalNext,
                    signalException: exception => { OnError(exception); }
                );
            }
            catch (Exception exception)
            {
                OnError(exception);
            }
            
            _completed = true;
            _signalCompletion();
        }
    }
}