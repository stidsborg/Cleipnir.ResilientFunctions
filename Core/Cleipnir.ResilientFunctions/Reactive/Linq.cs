using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Reactive.Awaiter;

namespace Cleipnir.ResilientFunctions.Reactive;

public static class Linq
{
    #region Non leaf operators

    public static IStream<TOut> Select<TIn, TOut>(this IStream<TIn> s, Func<TIn, TOut> mapper) 
        => s.WithOperator<TIn, TOut>((next, notify, _, _) => notify(mapper(next)));

    public static IStream<TFolded> Scan<T, TFolded>(this IStream<T> s, TFolded seed, Func<TFolded, T, TFolded> folder)
        => s.WithOperator<T, TFolded>(
            () =>
            {
                var curr = seed;
                return (next, notify, _, _) =>
                {
                    curr = folder(curr, next);
                    notify(curr);
                };
            });

    public static IStream<T> Where<T>(this IStream<T> s, Func<T, bool> filter) =>
        s.WithOperator<T, T>((next, notify, _, _) =>
        {
            if (filter(next))
                notify(next);
        });

    public static IStream<T> OfType<T>(this IStream<object> s) =>
        s.WithOperator<object, T>((next, notify, _, _) =>
        {
            if (next is T t)
                notify(t);
        });

    public static IStream<object> OfTypes<T1, T2>(this IStream<object> s) 
        => s.Where(m => m is T1 or T2);

    private static IStream<TOut> WithOperator<TIn, TOut>(this IStream<TIn> inner, Func<Operator<TIn, TOut>> operatorFunc)
        => new CustomOperator<TIn, TOut>(inner, operatorFunc);

    private static IStream<TOut> WithOperator<TIn, TOut>(this IStream<TIn> inner, Operator<TIn, TOut> @operator)
        => new CustomOperator<TIn, TOut>(inner, () => @operator);

    public static IStream<T> Take<T>(this IStream<T> s, int toTake)
    {
        if (toTake < 1)
            throw new ArgumentException("Must take a non-negative number of elements", nameof(toTake));

        return s.WithOperator<T, T>(
            () =>
            {
                var left = toTake;
                return (next, notify, completion, _) =>
                {
                    notify(next);
                    left--;
                    if (left == 0)
                        completion();
                };
            });
    }

    public static IStream<T> Skip<T>(this IStream<T> s, int toSkip)
        => s.WithOperator<T, T>(
            () =>
            {
                var left = toSkip;
                return (next, notify, _, _) =>
                {
                    if (left <= 0)
                        notify(next);
                    else
                        left--;
                };
            });

    public static IStream<T> Merge<T>(this IStream<T> stream1, IStream<T> stream2)
        => new MergeOperator<T>(stream1, stream2);

    #endregion

    #region Leaf operators

    public static Task<T> Last<T>(this IStream<T> s)
    {
        var tcs = new TaskCompletionSource<T>();
        var eventEmitted = false;
        var emittedEvent = default(T);
        
        var subscription = s.Subscribe(
            onNext: t =>
            {
                eventEmitted = true;
                emittedEvent = t;
            },
            onCompletion: () =>
            {
                if (!eventEmitted)
                    tcs.TrySetException(new NoResultException("No event was emitted before the stream completed"));
                else
                    tcs.TrySetResult(emittedEvent!);
            },
            onError: e => tcs.TrySetException(e)
        );
        
        subscription.DeliverExistingAndFuture();

        tcs.Task.ContinueWith(
            _ => subscription.Dispose(),
            TaskContinuationOptions.ExecuteSynchronously
        );
            
        return tcs.Task;
    }  
    
    public static async Task<T> Next<T>(this IStream<T> s) => await s.Take(1).Last();

    public static async Task<T> NextOrSuspend<T>(this IStream<T> s)
    {
        var tcs = new TaskCompletionSource<T>();
        var eventEmitted = false;
        var emittedEvent = default(T);
        
        var subscription = s.Subscribe(
            onNext: t =>
            {
                eventEmitted = true;
                emittedEvent = t;
            },
            onCompletion: () =>
            {
                if (!eventEmitted)
                    tcs.TrySetException(new NoResultException("No event was emitted before the stream completed"));
                else
                    tcs.TrySetResult(emittedEvent!);
            },
            onError: e => tcs.TrySetException(e)
        );
        
        var delivered = subscription.DeliverExisting();

        if (!tcs.Task.IsCompleted && !eventEmitted)
            throw new SuspendInvocationException(delivered);

        if (eventEmitted)
            tcs.TrySetResult(emittedEvent!);
        
        return await tcs.Task;
    }
    
    public static async Task<Result<T>> TryNextOrSuspend<T>(this IStream<T> s)
    {
        var tcs = new TaskCompletionSource<Result<T>>();
        var eventEmitted = false;
        var emittedEvent = default(T);
        
        var subscription = s.Subscribe(
            onNext: t =>
            {
                eventEmitted = true;
                emittedEvent = t;
            },
            onCompletion: () =>
            {
                if (!eventEmitted)
                    tcs.TrySetException(new NoResultException("No event was emitted before the stream completed"));
                else
                    tcs.TrySetResult(emittedEvent!);
            },
            onError: e => tcs.TrySetException(e)
        );
        
        var delivered = subscription.DeliverExisting();

        if (!tcs.Task.IsCompleted && !eventEmitted)
            tcs.SetResult(Suspend.Until(delivered).ToResult<T>());

        if (eventEmitted)
            tcs.TrySetResult(emittedEvent!);
        
        return await tcs.Task;
    }
    
    public static Task<List<T>> ToList<T>(this IStream<T> stream)
    {
        var tcs = new TaskCompletionSource<List<T>>();
        var list = new List<T>();
        var subscription = stream.Subscribe(
            onNext: t => list.Add(t),
            onCompletion: () => tcs.TrySetResult(list),
            onError: e => tcs.TrySetException(e)
        );
        subscription.DeliverExistingAndFuture();
        
        return tcs.Task;
    }

    #endregion
}