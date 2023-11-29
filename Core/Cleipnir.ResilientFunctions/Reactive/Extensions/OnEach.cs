using System;

namespace Cleipnir.ResilientFunctions.Reactive.Extensions;

internal class OnEach<T> : IDisposable
{
    private readonly ISubscription _subscription;

    public OnEach(IReactiveChain<T> s, Action<T> onNext, Action onCompletion, Action<Exception> onError) 
        => _subscription = s.Subscribe(onNext, onCompletion, onError);

    public void Start()
    {
        _subscription.DeliverExisting();
        _subscription.DeliverFuture();
    }
    
    public void Dispose() => _subscription.Dispose();
}

public static class OnEachExtension
{
    public static IDisposable OnEach<T>(
        this IReactiveChain<T> s,
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