using System;

namespace Cleipnir.ResilientFunctions.Reactive;

public interface IStream<out T>
{
    ISubscription Subscribe(
        Action<T> onNext, 
        Action onCompletion, 
        Action<Exception> onError, 
        int? subscriptionGroupId = null
    );
}