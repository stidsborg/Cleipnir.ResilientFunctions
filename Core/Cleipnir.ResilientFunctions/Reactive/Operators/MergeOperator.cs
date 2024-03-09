using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Reactive.Operators;

public class MergeOperator<T> : IReactiveChain<T>
{
    private readonly IReactiveChain<T> _chain1;
    private readonly IReactiveChain<T> _chain2;

    public MergeOperator(IReactiveChain<T> chain1, IReactiveChain<T> chain2)
    {
        _chain1 = chain1;
        _chain2 = chain2;
    }

    public ISubscription Subscribe(Action<T> onNext, Action onCompletion, Action<Exception> onError, ISubscriptionGroup? addToSubscriptionGroup = null)
        => new Subscription(_chain1, _chain2, onNext, onCompletion, onError, addToSubscriptionGroup);

    private class Subscription : ISubscription
    {
        private readonly Action<T> _onNext;
        private readonly Action _onCompletion;
        private readonly Action<Exception> _onError;

        private readonly ISubscription _subscription1;
        private bool _subscription1Completed;
        private readonly ISubscription _subscription2;
        private bool _subscription2Completed;
        private readonly EventSequencer<Either> _eventSequencer;

        private readonly object _sync = new();
        private bool _completed;

        public ISubscriptionGroup Group => _subscription1.Group;
        public IReactiveChain<object> Source { get; }
        public TimeSpan DefaultMessageSyncDelay { get; }

        public Subscription(
            IReactiveChain<T> inner1,
            IReactiveChain<T> inner2,
            Action<T> onNext, Action onCompletion, Action<Exception> onError,
            ISubscriptionGroup? addToSubscriptionGroup
        )
        {
            
            _onNext = onNext;
            _onCompletion = onCompletion;
            _onError = onError;

            _eventSequencer = new EventSequencer<Either>(HandleEither, onCompletion: () => {}, onError: _ => {});

            _subscription1 = inner1.Subscribe(
                onNext: next => _eventSequencer.HandleNext(
                    new Either(fromStream1: true, StreamEvent<T>.CreateFromNext(next))
                ),
                onCompletion: () => _eventSequencer.HandleNext(
                    new Either(fromStream1: true, StreamEvent<T>.CreateFromCompletion())
                ),
                onError: e => _eventSequencer.HandleNext(
                    new Either(fromStream1: true, StreamEvent<T>.CreateFromException(e))
                ), 
                addToSubscriptionGroup
            );
            _subscription2 = inner2.Subscribe(
                onNext: next => _eventSequencer.HandleNext(
                    new Either(fromStream1: false, StreamEvent<T>.CreateFromNext(next))
                ),
                onCompletion: () => _eventSequencer.HandleNext(
                    new Either(fromStream1: false, StreamEvent<T>.CreateFromCompletion())
                ),
                onError: e => _eventSequencer.HandleNext(
                    new Either(fromStream1: false, StreamEvent<T>.CreateFromException(e))
                ), 
                _subscription1.Group
            );

            Source = _subscription1.Source;
            TimeoutProvider = _subscription1.TimeoutProvider;
            DefaultMessageSyncDelay = _subscription1.DefaultMessageSyncDelay;
        }

        public ITimeoutProvider TimeoutProvider { get; }
        public Task SyncStore(TimeSpan maxSinceLastSynced) => _subscription1.SyncStore(maxSinceLastSynced); 

        public InterruptCount PushMessages() => _subscription1.PushMessages();

        private void HandleEither(Either either)
        {
            switch (either.StreamEvent.Status)
            {
                case StreamEventStatus.SignalNext:
                    SignalNext(either.StreamEvent.Next);
                    break;
                case StreamEventStatus.SignalCompletion:
                    if (either.FromStream1)
                        SignalCompletion1();
                    else
                        SignalCompletion2();
                    break;
                case StreamEventStatus.SignalError:
                    SignalError(either.StreamEvent.Error!);
                    break;
            }
        }
        
        private void SignalNext(T next)
        {
            lock (_sync)
                if (_completed) return;

            try
            {
                _onNext(next);
            }
            catch (Exception exception)
            {
                SignalError(exception);
            }
        }

        private void SignalError(Exception exception)
        {
            lock (_sync)
                if (_completed) return;
                else _completed = true;

            _onError(exception);

            _subscription1Completed = true;
            _subscription2Completed = true;
        }

        private void SignalCompletion1()
        {
            lock (_sync)
            {
                if (_completed || _subscription1Completed) return;
                _subscription1Completed = true;
                
                if (!(_subscription1Completed && _subscription2Completed)) return;
                
                _completed = true;
            }

            _onCompletion();
        }
        
        private void SignalCompletion2()
        {
            lock (_sync)
            {
                if (_subscription2Completed || _completed) return;
                _subscription2Completed = true;
                if (!(_subscription1Completed && _subscription2Completed)) return;
                
                _completed = true;
            }

            _onCompletion();
        }

        private readonly struct Either
        {
            public StreamEvent<T> StreamEvent { get; }
            public bool FromStream1 { get; }

            public Either(bool fromStream1, StreamEvent<T> streamEvent)
            {
                FromStream1 = fromStream1;
                StreamEvent = streamEvent;
            }
        }
    }
}