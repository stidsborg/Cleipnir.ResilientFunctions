using System;

namespace Cleipnir.ResilientFunctions.Reactive;

public interface IStream<out T>
{
    public int TotalEventCount { get; }
    ISubscription Subscribe(Action<T> onNext, Action onCompletion, Action<Exception> onError);
}