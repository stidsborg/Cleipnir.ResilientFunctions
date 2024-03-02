using System;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions.Reactive.Origin;

internal class SubscriptionGroup
{
    private readonly int _subscriptionGroupId;
    private readonly Source _source;

    private Status _status = Status.Created;
    private ImmutableArray<Subscription> _subscriptions = ImmutableArray<Subscription>.Empty;
    private readonly EmittedEvents _emittedEvents;
    private readonly ITimeoutProvider _timeoutProvider;
    private int _skip;
    private readonly object _sync = new();

    private Action<int> UnsubscribeToEvents { get; }

    public int EmittedFromSource
    {
        get
        {
            lock (_sync)
                return _skip;
        }
    }

    public SubscriptionGroup(
        int subscriptionGroupId,
        EmittedEvents emittedEvents,
        ITimeoutProvider timeoutProvider,
        Source source,
        Action<int> unsubscribeToEvents)
    {
        _subscriptionGroupId = subscriptionGroupId;
        _emittedEvents = emittedEvents;
        _timeoutProvider = timeoutProvider;
        UnsubscribeToEvents = unsubscribeToEvents;
        _source = source;
    }

    public void DeliverFutureEvents()
    {
        lock (_sync)
            if (_status == Status.Created)
                _status = Status.Activated;
            else
                return;

        DeliverOutstandingEvents();
    }

    public void DeliverExisting()
    {
        lock (_sync)
            if (_status != Status.Created)
                throw new InvalidOperationException($"Cannot deliver existing events on subscription with status: '{_status}'");

        var toEmits = _emittedEvents.GetEvents(_skip);
        _skip += toEmits.Length;

        foreach (var toEmit in toEmits)
        foreach (var subscription in _subscriptions)
        {
            if (toEmit.Completion)
                subscription.OnCompletion();
            else if (toEmit.EmittedException != null)
                subscription.OnError(toEmit.EmittedException!);
            else
                subscription.OnNext(toEmit.Event!);
        }
    }

    public void DeliverOutstandingEvents()
    {
        lock (_sync)
            if (_status != Status.Activated)
                return;
            else
                _status = Status.Emitting;

        try
        {
            while (true)
            {
                Span<EmittedEvent> toEmits;
                lock (_sync)
                {
                    toEmits = _emittedEvents.GetEvents(_skip);
                    _skip += toEmits.Length;

                    if (toEmits.IsEmpty)
                        return;
                }

                foreach (var toEmit in toEmits)
                foreach (var subscription in _subscriptions)
                {
                    if (toEmit.Completion)
                        subscription.OnCompletion();
                    else if (toEmit.EmittedException != null)
                        subscription.OnError(toEmit.EmittedException!);
                    else
                        subscription.OnNext(toEmit.Event!);
                }
            }
        }
        catch (Exception)
        {
            Dispose();
            throw;
        }
        finally
        {
            lock (_sync)
                _status = _status == Status.Disposing
                    ? Status.Disposed
                    : Status.Activated;
        }
    }

    public ISubscription AddSubscription(Action<object> onNext, Action onCompletion, Action<Exception> onError)
    {
        var subscription = new Subscription(onNext, onCompletion, onError, subscriptionGroup: this, _timeoutProvider, _source);
        _subscriptions = _subscriptions.Add(subscription);
        return subscription;
    }

    public async Task StopDelivering()
    {
        Dispose();

        while (true)
        {
            lock (_sync)
                if (_status == Status.Disposed)
                    return;

            await Task.Delay(10);
        }
    }

    private void Unsubscribe(Subscription subscription)
    {
        lock (_sync)
        {
            _subscriptions = _subscriptions.Remove(subscription);
            if (_subscriptions.Length > 0)
                return;
        }

        Dispose();
    }

    private void Dispose()
    {
        lock (_sync)
            switch (_status)
            {
                case Status.Disposing:
                case Status.Disposed:
                    return;
                case Status.Created:
                case Status.Activated:
                    _status = Status.Disposed;
                    break;
                case Status.Emitting:
                    _status = Status.Disposing;
                    break;
            }

        UnsubscribeToEvents(_subscriptionGroupId);
    }

    private enum Status
    {
        Created,
        Activated,
        Emitting,
        Disposing,
        Disposed
    }

    private class Subscription : ISubscription
    {
        public Action<object> OnNext { get; }
        public Action OnCompletion { get; }
        public Action<Exception> OnError { get; }

        public int EmittedFromSource => _subscriptionGroup.EmittedFromSource;
        public IReactiveChain<object> Source { get; }

        private readonly SubscriptionGroup _subscriptionGroup;

        public Subscription(
            Action<object> onNext, Action onCompletion, Action<Exception> onError,
            SubscriptionGroup subscriptionGroup,
            ITimeoutProvider timeoutProvider,
            IReactiveChain<object> source)
        {
            OnNext = onNext;
            OnCompletion = onCompletion;
            OnError = onError;
            Source = source;
            _subscriptionGroup = subscriptionGroup;
            TimeoutProvider = timeoutProvider;
        }

        public ITimeoutProvider TimeoutProvider { get; }
        public int SubscriptionGroupId => _subscriptionGroup._subscriptionGroupId;
        public void DeliverFuture() => _subscriptionGroup.DeliverFutureEvents();
        public void DeliverExisting() => _subscriptionGroup.DeliverExisting();
        public Task StopDelivering() => _subscriptionGroup.StopDelivering();

        public void Dispose() => _subscriptionGroup.Unsubscribe(subscription: this);
    }
}