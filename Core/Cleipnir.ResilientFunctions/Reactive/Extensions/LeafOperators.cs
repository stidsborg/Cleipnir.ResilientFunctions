using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Utilities;

namespace Cleipnir.ResilientFunctions.Reactive.Extensions;

public static class LeafOperators
{
    #region ToList

    public static async Task<List<T>> SuspendUntilToList<T>(this IReactiveChain<T> s, TimeSpan? maxWait = null)
    {
        if (maxWait < TimeSpan.Zero && maxWait != Timeout.InfiniteTimeSpan)
            throw new ArgumentException("Timeout must be non-negative", nameof(maxWait));

        var emits = new List<T>();
        var error = default(Exception);
        var completed = false;
        
        var tcs = new TaskCompletionSource();
        using var subscription = s.Subscribe(
            onNext: next => emits.Add(next),
            onCompletion: () =>
            {
                completed = true;
                tcs.TrySetResult();
            },
            onError: e =>
            {
                error = e;
                tcs.TrySetResult();
            });

        subscription.DeliverExisting();
        
        //short-circuits
        if (error != null)
            throw error;
        if (completed)
            return emits;
        if (maxWait == null || maxWait.Value == TimeSpan.Zero)
            throw new SuspendInvocationException(subscription.EmittedFromSource);
        
        //do slow-path
        subscription.DeliverFuture();
        
        using var cts = new CancellationTokenSource();
        await Task.WhenAny(tcs.Task, Task.Delay(maxWait.Value, cts.Token));
        cts.Cancel();

        await subscription.StopDelivering();

        if (error != null)
            throw error;
        if (completed)
            return emits;
        
        throw new SuspendInvocationException(subscription.EmittedFromSource);
    }

    public static Task<List<T>> ToList<T>(this IReactiveChain<T> s)
    {
        var emits = new List<T>();
        var tcs = new TaskCompletionSource<List<T>>();
        
        var subscription = s.Subscribe(
            onNext: next => emits.Add(next),
            onCompletion: () => tcs.TrySetResult(emits),
            onError: e => tcs.TrySetException(e)
        );

        subscription.DeliverExisting();
        subscription.DeliverFuture();

        tcs.Task.ContinueWith(
            _ => subscription.Dispose(),
            TaskContinuationOptions.ExecuteSynchronously
        );
        
        return tcs.Task;
    }
    
    #endregion
    
    #region Completion
    
    public static Task<List<T>> SuspendUntilCompletion<T>(this IReactiveChain<T> s, TimeSpan? maxWait = null)
        => s.SuspendUntilToList(maxWait);

    public static Task<List<T>> Completion<T>(this IReactiveChain<T> s)
        => s.ToList();
    
    #endregion

    public static List<T> Existing<T>(this IReactiveChain<T> s)
        => Existing(s, out _);

    public static List<T> Existing<T>(this IReactiveChain<T> s, out int emittedFromSource)
        => Existing(s, out emittedFromSource, out _);
    
    public static List<T> Existing<T>(this IReactiveChain<T> s, out int emittedFromSource, out bool streamCompleted)
    {
        var completed = false;
        var error = default(Exception);
        var list = new List<T>();
        using var subscription = s.Subscribe(
            onNext: t => list.Add(t),
            onCompletion: () => completed = true,
            onError: e => error = e
        );
        
        subscription.DeliverExisting();
        emittedFromSource = subscription.EmittedFromSource;
        
        streamCompleted = completed;
        
        if (error != null)
            throw error;
        
        return list;
    }

    #region First
    public static Task<T> SuspendUntilFirst<T>(this IReactiveChain<T> s, TimeSpan? maxWait = null)
        => s.SuspendUntilFirstOrNone(maxWait: maxWait)
            .SelectAsync(o => o.HasValue ? o.Value : throw new NoResultException());
    public static Task<T?> SuspendUntilFirstOrDefault<T>(this IReactiveChain<T> s, TimeSpan? maxWait = null)
        => SuspendUntilFirstOrNone(s, maxWait)
            .SelectAsync(o => o.HasValue ? o.Value : default);
    public static Task<Option<T>> SuspendUntilFirstOrNone<T>(this IReactiveChain<T> s, TimeSpan? maxWait = null)
        => SuspendUntilFirsts(s, count: 1, maxWait)
            .SelectAsync(emits => emits.Any()
                ? new Option<T>(emits.Single())
                : Option<T>.NoValue
            );
    public static Task<T> SuspendUntilFirstOfType<T>(this IReactiveChain<object> s, TimeSpan? maxWait = null)
        => s.OfType<T>().SuspendUntilFirst(maxWait);
    public static Task<List<T>> SuspendUntilFirsts<T>(this IReactiveChain<T> s, int count, TimeSpan? maxWait = null)
        => s.Take(count).SuspendUntilToList(maxWait);
    
    public static Task<T> First<T>(this IReactiveChain<T> s)
        => FirstOrNone(s)
            .SelectAsync(o => o.HasValue ? o.Value : throw new NoResultException());
    public static Task<T?> FirstOrDefault<T>(this IReactiveChain<T> s)
        => FirstOrNone(s)
            .SelectAsync(o => o.HasValue ? o.Value : default);
    public static Task<Option<T>> FirstOrNone<T>(this IReactiveChain<T> s)
        => Firsts(s, count: 1)
            .SelectAsync(
                l => l.Any() ? new Option<T>(l.First()) : Option<T>.NoValue
            );
    public static Task<T> FirstOfType<T>(this IReactiveChain<object> s)
        => s.OfType<T>().First();

    public static Task<Option<T>> FirstOfType<T>(this IReactiveChain<object> s, string timeoutId, DateTime expiresAt)
        => s.OfType<T>().TakeUntilTimeout(timeoutId, expiresAt).FirstOrNone();
    public static Task<Option<T>> FirstOfType<T>(this IReactiveChain<object> s, string timeoutId, TimeSpan expiresIn)
        => s.OfType<T>().TakeUntilTimeout(timeoutId, expiresIn).FirstOrNone();

    public static Task<T> FirstOf<T>(this IReactiveChain<object> s) => s.FirstOfType<T>();
    
    public static Task<List<T>> Firsts<T>(this IReactiveChain<T> s, int count)
        => s.Take(count).ToList();
    
    #endregion
    
    #region Last

    public static Task<T> SuspendUntilLast<T>(this IReactiveChain<T> s, TimeSpan? maxWait = null)
        => s.SuspendUntilLastOrNone(maxWait)
            .SelectAsync(o => o.HasValue ? o.Value : throw new NoResultException());
    public static Task<T?> SuspendUntilLastOrDefault<T>(this IReactiveChain<T> s, TimeSpan? maxWait = null)
        => s.SuspendUntilLastOrNone(maxWait)
            .SelectAsync(o => o.HasValue ? o.Value : default);
    public static Task<Option<T>> SuspendUntilLastOrNone<T>(this IReactiveChain<T> s, TimeSpan? maxWait = null)
        => s.SuspendUntilCompletion(maxWait)
            .SelectAsync(l => l.Any() ? new Option<T>(l.Last()) : Option<T>.NoValue);
    public static Task<List<T>> SuspendUntilLasts<T>(this IReactiveChain<T> s, int count, TimeSpan? maxWait = null)
        => s.SuspendUntilCompletion(maxWait).SelectAsync(l => l.TakeLast(count).ToList());

    public static Task<List<T>> Lasts<T>(this IReactiveChain<T> s, int count) 
        => s.ToList().SelectAsync(l => l.TakeLast(count).ToList());
    
    public static Task<T> Last<T>(this IReactiveChain<T> s)
        => LastOrNone(s)
            .SelectAsync(o => o.HasValue ? o.Value : throw new NoResultException());
    public static Task<T?> LastOrDefault<T>(this IReactiveChain<T> s)
        => LastOrNone(s)
            .SelectAsync(o => o.HasValue ? o.Value : default);
    public static Task<Option<T>> LastOrNone<T>(this IReactiveChain<T> s)
        => Lasts(s, count: 1)
            .SelectAsync(l =>
                l.Any()
                    ? new Option<T>(l.First())
                    : Option<T>.NoValue
            );

    public static Task<T> LastOfType<T>(this IReactiveChain<object> s)
        => s.OfType<T>().Last();
    public static Task<T> LastOf<T>(this IReactiveChain<object> s) => s.LastOfType<T>();
    
    public static Task<Option<T>> LastOfType<T>(this IReactiveChain<object> s, string timeoutId, DateTime expiresAt)
        => s.OfType<T>().TakeUntilTimeout(timeoutId, expiresAt).LastOrNone();
    public static Task<Option<T>> LastOfType<T>(this IReactiveChain<object> s, string timeoutId, TimeSpan expiresIn)
        => s.OfType<T>().TakeUntilTimeout(timeoutId, expiresIn).LastOrNone();
    
    #endregion

    #region Suspend

    public static async Task SuspendUntil(this Messages s, string timeoutEventId, DateTime resumeAt)
    {
        var timeoutEmitted = false;
        using var subscription = s
            .OfType<TimeoutEvent>()
            .Where(t => t.TimeoutId == timeoutEventId)
            .Take(1)
            .Subscribe(
                onNext: _ => timeoutEmitted = true,
                onCompletion: () => { },
                onError: _ => { }
            );

        subscription.DeliverExisting();
        
        if (timeoutEmitted)
            return;

        await subscription.TimeoutProvider.RegisterTimeout(timeoutEventId, resumeAt);
        throw new SuspendInvocationException(subscription.EmittedFromSource);
    }

    public static Task SuspendFor(this Messages s, string timeoutEventId, TimeSpan resumeAfter)
        => s.SuspendUntil(timeoutEventId, DateTime.UtcNow.Add(resumeAfter));

    #endregion
}