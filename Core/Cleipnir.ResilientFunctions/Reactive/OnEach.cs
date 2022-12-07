using System;

namespace Cleipnir.ResilientFunctions.Reactive;

internal class OnEach<T> : IDisposable
{
    private readonly ISubscription _subscription;

    public OnEach(IStream<T> s, Action<T> onNext, Action onCompletion, Action<Exception> onError) 
        => _subscription = s.Subscribe(onNext, onCompletion, onError);

    public void Start() => _subscription.DeliverExistingAndFuture();
    public void Dispose() => _subscription.Dispose();
}

public static class OnEachExtension
{
    public static IDisposable OnEach<T>(
        this IStream<T> s,
        Action<T> onNext,
        Action? onCompletion = null,
        Action<Exception>? onError = null
    )
    {
        var @operator = new OnEach<T>(s, onNext, onCompletion ?? (() => { }), onError ?? (_ => { }));
        @operator.Start();
        return @operator;
    } 
}