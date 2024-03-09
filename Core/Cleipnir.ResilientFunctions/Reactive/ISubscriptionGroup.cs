using System;

namespace Cleipnir.ResilientFunctions.Reactive;

public interface ISubscriptionGroup : ISubscription
{
    void Add(Action<object> onNext, Action onCompletion, Action<Exception> onError);
}