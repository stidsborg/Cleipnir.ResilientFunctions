using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Messaging;

public static class ReactiveOperators
{
    public static Task<Option<T>> NextEventOption<T>(this IObservable<T> observable, int maxWaitMs)
    {
        var tcs = new TaskCompletionSource<Option<T>>();
        var cancellationToken = new CancellationTokenSource();
        
        observable.Subscribe(
            onNext: t =>
            {
                tcs.TrySetResult(new Option<T>(t));
                cancellationToken.Cancel();
            },
            onError: e =>
            {
                tcs.TrySetException(e);
                cancellationToken.Cancel();
            },
            onCompleted: () =>
            {
                tcs.TrySetException(new InvalidOperationException("Observable completed without result"));
                cancellationToken.Cancel();
            },
            cancellationToken.Token
        );

        Task.Delay(maxWaitMs, cancellationToken.Token)
            .ContinueWith(_ => tcs.TrySetResult(Option<T>.NoValue), cancellationToken.Token);

        return tcs.Task;
    }
    
    public static Task<T> NextEvent<T>(this IObservable<T> observable, int maxWaitMs)
    {
        var tcs = new TaskCompletionSource<T>();
        var cancellationToken = new CancellationTokenSource();
        
        observable.Subscribe(
            onNext: t =>
            {
                tcs.TrySetResult(t);
                cancellationToken.Cancel();
            },
            onError: e =>
            {
                tcs.TrySetException(e);
                cancellationToken.Cancel();
            },
            onCompleted: () =>
            {
                tcs.TrySetException(new InvalidOperationException("Observable completed without result"));
                cancellationToken.Cancel();
            },
            cancellationToken.Token
        );

        Task.Delay(maxWaitMs, cancellationToken.Token)
            .ContinueWith(_ => 
                tcs.TrySetException(new TimeoutException("Event was not emitted within threshold")), 
                cancellationToken.Token
            );

        return tcs.Task;
    }
    
    public static Task<List<T>> AllEvents<T>(this IObservable<T> observable, int maxWaitMs)
    {
        var tcs = new TaskCompletionSource<List<T>>();
        var cancellationToken = new CancellationTokenSource();
        var events = new List<T>();
        
        observable.Subscribe(
            onNext: t => events.Add(t),
            onError: e =>
            {
                tcs.TrySetException(e);
                cancellationToken.Cancel();
            },
            onCompleted: () =>
            {
                tcs.TrySetResult(events);
                cancellationToken.Cancel();
            },
            cancellationToken.Token
        );

        Task.Delay(maxWaitMs, cancellationToken.Token)
            .ContinueWith(_ => 
                    tcs.TrySetException(new TimeoutException("Observable did not complete within threshold")), 
                cancellationToken.Token
            );

        return tcs.Task;
    }
    
    public static Task<T> NextEvent<T>(this IObservable<T> observable)
    {
        var tcs = new TaskCompletionSource<T>();
        var cancellationToken = new CancellationTokenSource();

        observable.Subscribe(
            onNext: t =>
            {
                tcs.TrySetResult(t);
                cancellationToken.Cancel();
            },
            onCompleted: () =>
                tcs.TrySetException(new InvalidOperationException("Observable completed without result")),
            onError: e => tcs.TrySetException(e)
        );
        
        return tcs.Task;
    }
}