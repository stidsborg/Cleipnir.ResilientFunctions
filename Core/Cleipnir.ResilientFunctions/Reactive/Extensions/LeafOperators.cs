using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;

namespace Cleipnir.ResilientFunctions.Reactive.Extensions;

public static class LeafOperators
{
    public static async Task<List<T>> ToList<T>(this IReactiveChain<T> s, TimeSpan? maxWait = null)
    {
        if (maxWait < TimeSpan.Zero)
            throw new ArgumentException("Timeout must be non-negative", nameof(maxWait));
        
        var emits = new List<T>();
        var tcs = new TaskCompletionSource<List<T>>();
        var subscription = s.Subscribe(
            onNext: next => emits.Add(next),
            onCompletion: () => tcs.TrySetResult(emits),
            onError: e => tcs.TrySetException(e)
        );
        await subscription.Initialize();
        
        subscription.PushMessages();

        //short-circuit
        if (tcs.Task.IsCompleted)
        {
            await subscription.CancelTimeout();
            return await tcs.Task;
        }

        await subscription.RegisterTimeout();
        
        maxWait ??= subscription.DefaultMessageMaxWait;
        if (maxWait == TimeSpan.Zero)
            throw new SuspendInvocationException();

        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        while (!tcs.Task.IsCompleted && subscription.IsWorkflowRunning && stopWatch.Elapsed < maxWait)
        {
            await Task.Delay(subscription.DefaultMessageSyncDelay);
            await subscription.SyncStore(maxSinceLastSynced: subscription.DefaultMessageSyncDelay);
            subscription.PushMessages();
        }

        if (tcs.Task.IsCompleted)
        {
            await subscription.CancelTimeout();
            return await tcs.Task;
        }
        
        throw new SuspendInvocationException();
    }
    
    internal static List<T> Existing<T>(this IReactiveChain<T> s, out bool streamCompleted)
    {
        var completed = false;
        var error = default(Exception);
        var list = new List<T>();
        var subscription = s.Subscribe(
            onNext: t => list.Add(t),
            onCompletion: () => completed = true,
            onError: e => error = e
        );

        subscription.PushMessages();
        streamCompleted = completed;
        
        if (error != null)
            throw error;
        
        return list;
    }
}