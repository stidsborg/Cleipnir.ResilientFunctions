using System;

namespace Cleipnir.ResilientFunctions.Reactive;

public interface IStream<out T>
{
    IDisposable Subscribe(Action<T> onNext, Action onCompletion, Action<Exception> onError);
}