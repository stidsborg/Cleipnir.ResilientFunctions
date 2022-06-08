namespace Cleipnir.ResilientFunctions.Messaging.Core;

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
            cancellationToken.Token
        );

        Task.Delay(maxWaitMs, cancellationToken.Token)
            .ContinueWith(_ => 
                tcs.TrySetException(new TimeoutException("Event was not emitted within threshold")), 
                cancellationToken.Token
            );

        return tcs.Task;
    }
}