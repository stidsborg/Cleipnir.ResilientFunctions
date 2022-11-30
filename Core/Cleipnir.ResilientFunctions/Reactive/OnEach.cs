using System;

namespace Cleipnir.ResilientFunctions.Reactive;

public class OnEach<T> : IDisposable
{
    private readonly IDisposable _subscription;
        
    public OnEach(IStream<T> s, Action<T> onNext, Action onCompletion, Action<Exception> onError) 
        => _subscription = s.Subscribe(onNext, onCompletion, onError);

    public void Dispose() => _subscription.Dispose();
}

public static class OnEachExtension
{
    public static IDisposable OnEach<T>(
        this IStream<T> s,
        Action<T> onNext,
        Action? onCompletion = null,
        Action<Exception>? onError = null
    ) => new OnEach<T>(s, onNext, onCompletion ?? (() => { }), onError ?? (_ => { }));
}