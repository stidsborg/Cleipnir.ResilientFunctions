﻿using System;
using Cleipnir.ResilientFunctions.CoreRuntime;

namespace Cleipnir.ResilientFunctions.Reactive.Operators;

public class MergeOperator<T> : IStream<T>
{
    private readonly IStream<T> _stream1;
    private readonly IStream<T> _stream2;

    public MergeOperator(IStream<T> stream1, IStream<T> stream2)
    {
        _stream1 = stream1;
        _stream2 = stream2;
    }

    public ISubscription Subscribe(Action<T> onNext, Action onCompletion, Action<Exception> onError, int? subscriptionGroupId = null)
        => new Subscription(_stream1, _stream2, onNext, onCompletion, onError, subscriptionGroupId);

    private class Subscription : ISubscription
    {
        private readonly Action<T> _onNext;
        private readonly Action _onCompletion;
        private readonly Action<Exception> _onError;

        private readonly ISubscription _subscription1;
        private bool _subscription1Completed;
        private readonly ISubscription _subscription2;
        private bool _subscription2Completed;
        private readonly StreamEventSequencer<Either> _streamEventSequencer;

        private readonly object _sync = new();
        private bool _completed;

        public IStream<object> Source { get; }

        public Subscription(
            IStream<T> inner1,
            IStream<T> inner2,
            Action<T> onNext, Action onCompletion, Action<Exception> onError,
            int? subscriptionGroupId)
        {
            _onNext = onNext;
            _onCompletion = onCompletion;
            _onError = onError;

            _streamEventSequencer = new StreamEventSequencer<Either>(HandleEither, onCompletion: () => {}, onError: _ => {});

            _subscription1 = inner1.Subscribe(
                onNext: next => _streamEventSequencer.HandleNext(
                    new Either(fromStream1: true, StreamEvent<T>.CreateFromNext(next))
                ),
                onCompletion: () => _streamEventSequencer.HandleNext(
                    new Either(fromStream1: true, StreamEvent<T>.CreateFromCompletion())
                ),
                onError: e => _streamEventSequencer.HandleNext(
                    new Either(fromStream1: true, StreamEvent<T>.CreateFromException(e))
                ), 
                subscriptionGroupId
            );
            _subscription2 = inner2.Subscribe(
                onNext: next => _streamEventSequencer.HandleNext(
                    new Either(fromStream1: false, StreamEvent<T>.CreateFromNext(next))
                ),
                onCompletion: () => _streamEventSequencer.HandleNext(
                    new Either(fromStream1: false, StreamEvent<T>.CreateFromCompletion())
                ),
                onError: e => _streamEventSequencer.HandleNext(
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
        public void DeliverExistingAndFuture() => _subscription1.DeliverExistingAndFuture();
        public int DeliverExisting() => _subscription1.DeliverExisting();

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
                if (_subscription1Completed) return;
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
                if (_subscription2Completed) return;
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