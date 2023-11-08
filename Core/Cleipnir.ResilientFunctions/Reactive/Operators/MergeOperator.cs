using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;

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

    public ISubscription Subscribe(Action<T> onNext, Action onCompletion, Action<Exception> onError, int? subscriptionGroupId = null)
        => new Subscription(_chain1, _chain2, onNext, onCompletion, onError, subscriptionGroupId);

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

        public int EmittedFromSource => _subscription1.EmittedFromSource;
        public IReactiveChain<object> Source { get; }

        public Subscription(
            IReactiveChain<T> inner1,
            IReactiveChain<T> inner2,
            Action<T> onNext, Action onCompletion, Action<Exception> onError,
            int? subscriptionGroupId)
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
                subscriptionGroupId
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
                _subscription1.SubscriptionGroupId
            );

            Source = _subscription1.Source;
            SubscriptionGroupId = _subscription1.SubscriptionGroupId;
            TimeoutProvider = _subscription1.TimeoutProvider;
        }

        public ITimeoutProvider TimeoutProvider { get; }
        public int SubscriptionGroupId { get; }
        public void DeliverFuture() => _subscription1.DeliverFuture();
        public void DeliverExisting() => _subscription1.DeliverExisting();
        public Task StopDelivering() => _subscription1.StopDelivering();

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
            _subscription1.Dispose();
            _subscription2Completed = true;
            _subscription2.Dispose();
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

        public void Dispose()
        {
            _subscription1.Dispose();
            _subscription2.Dispose();
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