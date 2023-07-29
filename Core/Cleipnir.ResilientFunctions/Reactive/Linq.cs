﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Awaiter;
using Cleipnir.ResilientFunctions.Reactive.Operators;

namespace Cleipnir.ResilientFunctions.Reactive;

public static class Linq
{
    #region Non leaf operators

    public static IReactiveChain<TOut> Select<TIn, TOut>(this IReactiveChain<TIn> s, Func<TIn, TOut> mapper) 
        => s.WithOperator<TIn, TOut>((next, notify, _, _) => notify(mapper(next)));

    public static IReactiveChain<TFolded> Scan<T, TFolded>(this IReactiveChain<T> s, TFolded seed, Func<TFolded, T, TFolded> folder)
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

    public static IReactiveChain<T> Where<T>(this IReactiveChain<T> s, Func<T, bool> filter) =>
        s.WithOperator<T, T>((next, notify, _, _) =>
        {
            if (filter(next))
                notify(next);
        });

    public static IReactiveChain<T> OfType<T>(this IReactiveChain<object> s) =>
        s.WithOperator<object, T>((next, notify, _, _) =>
        {
            if (next is T t)
                notify(t);
        });

    public static IReactiveChain<Either<T1, T2>> OfTypes<T1, T2>(this IReactiveChain<object> s)
        => s.WithOperator<object, Either<T1, T2>>(
            () => (next, notify, completion, exception) =>
            {
                if (next is T1 t1)
                    notify(Either<T1, T2>.CreateFirst(t1));
                else if (next is T2 t2)
                    notify(Either<T1, T2>.CreateSecond(t2));
            }
        );
    
    public static IReactiveChain<Either<T1, T2, T3>> OfTypes<T1, T2, T3>(this IReactiveChain<object> s)
        => s.WithOperator<object, Either<T1, T2, T3>>(
            () => (next, notify, completion, exception) =>
            {
                if (next is T1 t1)
                    notify(Either<T1, T2, T3>.CreateFirst(t1));
                else if (next is T2 t2)
                    notify(Either<T1, T2, T3>.CreateSecond(t2));
                else if (next is T3 t3)
                    notify(Either<T1, T2, T3>.CreateThird(t3));
            }
        );

    private static IReactiveChain<TOut> WithOperator<TIn, TOut>(this IReactiveChain<TIn> inner, Func<Operator<TIn, TOut>> operatorFunc, OnCompletion<TOut>? handleCompletion = null)
        => new CustomOperator<TIn, TOut>(inner, operatorFunc, handleCompletion);

    private static IReactiveChain<TOut> WithOperator<TIn, TOut>(this IReactiveChain<TIn> inner, Operator<TIn, TOut> @operator, OnCompletion<TOut>? handleCompletion = null)
        => new CustomOperator<TIn, TOut>(inner, () => @operator, handleCompletion);

    public static IReactiveChain<T> Take<T>(this IReactiveChain<T> s, int toTake)
    {
        if (toTake < 1)
            throw new ArgumentException("Must take a positive number of elements", nameof(toTake));

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
    
    public static IReactiveChain<T> TakeUntil<T>(this IReactiveChain<T> s, Func<T, bool> predicate)
    {
        return s.WithOperator<T, T>(
            () =>
            {
                var completed = false; 
               
                return (next, notify, complete, _) =>
                {
                    if (completed) return;
                    if (!predicate(next))
                        notify(next);
                    else
                    {
                        complete();
                        completed = true;
                    }
                };
            });
    }

    public static IReactiveChain<T> Skip<T>(this IReactiveChain<T> s, int toSkip)
    {
        if (toSkip < 0)
            throw new ArgumentException("Must take a non-negative number of elements", nameof(toSkip));

        return s.WithOperator<T, T>(
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
    }
    
    public static IReactiveChain<List<T>> Buffer<T>(this IReactiveChain<T> s, int bufferSize) 
        => new BufferOperator<T>(s, bufferSize);

    public static IReactiveChain<List<T>> Chunk<T>(this IReactiveChain<T> s, int size) => Buffer(s, size);

    public static IReactiveChain<T> Merge<T>(this IReactiveChain<T> stream1, IReactiveChain<T> stream2)
        => new MergeOperator<T>(stream1, stream2);

    #endregion

    #region Leaf operators

    public static Task<T> Last<T>(this IReactiveChain<T> s)
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

    public static Task<List<T>> ToList<T>(this IReactiveChain<T> s)
    {
        var tcs = new TaskCompletionSource<List<T>>();
        var list = new List<T>();
        var subscription = s.Subscribe(
            onNext: t => list.Add(t),
            onCompletion: () => tcs.TrySetResult(list),
            onError: e => tcs.TrySetException(e)
        );
        subscription.DeliverExistingAndFuture();

        return tcs.Task;
    }

    public static List<T> PullExisting<T>(this IReactiveChain<T> s)
    {
        var tcs = new TaskCompletionSource<List<T>>();
        var list = new List<T>();
        var subscription = s.Subscribe(
            onNext: t => list.Add(t),
            onCompletion: () => tcs.TrySetResult(list),
            onError: e => tcs.TrySetException(e)
        );
        subscription.DeliverExisting();
        subscription.Dispose();

        return list;
    }

    // ** NEXT RELATED OPERATORS ** //
    public static async Task<T> Next<T>(this IReactiveChain<T> s) => await s.Take(1).Last();
    public static Task<T> Next<T>(this IReactiveChain<T> s, int maxWaitMs)
        => s.Next(TimeSpan.FromMilliseconds(maxWaitMs));
    public static Task<T> Next<T>(this IReactiveChain<T> s, TimeSpan maxWait)
    {
        var tcs = new TaskCompletionSource<T>();
        _ = s.Take(1).Last().ContinueWith(t => tcs.TrySetResult(t.Result));
        _ = Task.Delay(maxWait).ContinueWith(_ => tcs.TrySetException(new TimeoutException($"Event was not emitted within threshold of {maxWait}")));

        return tcs.Task;
    }

    public static bool TryNext<T>(this IReactiveChain<T> s, out T? next)
        => TryNext(s, out next, out _);
    public static bool TryNext<T>(this IReactiveChain<T> s, out T? next, out int totalEventSourceCount)
    {
        var success = false;
        var t = default(T);
        var subscription = s.Subscribe(
            onNext: tt =>
            {
                if (success) return;
                
                t = tt;
                success = true;
            },
            onCompletion: () => { },
            onError: _ => { }
        );

        totalEventSourceCount = subscription.DeliverExisting();
        subscription.Dispose();
        next = t;
        return success;
    }

    public static Task<T> NextOfType<T>(this IReactiveChain<object> s)
        => s.OfType<T>().Next();
    public static Task<T> NextOfType<T>(this IReactiveChain<object> s, TimeSpan maxWait)
        => s.OfType<T>().Next(maxWait);
    public static bool TryNextOfType<T>(this IReactiveChain<object> s, out T? next)
        => s.TryNextOfType(out next, out _);
    public static bool TryNextOfType<T>(this IReactiveChain<object> s, out T? next, out int totalEventSourceCount)
        => s.OfType<T>().TryNext(out next, out totalEventSourceCount);
    
    public static Task<T> SuspendUntilNext<T>(this IReactiveChain<T> s, TimeSpan waitBeforeSuspension)
    {
        var tcs = new TaskCompletionSource<T>();
        var sync = new object();
        var waitForNextElapsed = false;
        var completed = false;
        
        var subscription = s.Subscribe(
            onNext: t =>
            {
                lock (sync)
                {
                    if (waitForNextElapsed || completed) return;
                    completed = true;
                }

                tcs.TrySetResult(t);
            },
            onCompletion: () =>
            {
                lock (sync)
                {
                    if (waitForNextElapsed || completed) return;
                    completed = true;
                }
                
                tcs.TrySetException(new NoResultException("No event was emitted before the stream completed"));
            },
            onError: e =>
            {
                lock (sync)
                {
                    if (waitForNextElapsed || completed) return;
                    completed = true;
                }
                
                tcs.TrySetException(e);
            });

        subscription.DeliverExistingAndFuture();

        _ = Task.Delay(waitBeforeSuspension).ContinueWith(_ =>
        {
            lock (sync)
            {
                if (completed) return;
                waitForNextElapsed = true;
            }

            subscription.Dispose();

            SuspendUntilNext(s).ContinueWith(task =>
            {
                if (task.IsCompletedSuccessfully)
                    tcs.TrySetResult(task.Result);
                else
                    tcs.TrySetException(task.Exception!.InnerException!);
            });
        });
        
        _ = tcs.Task.ContinueWith(_ => subscription.Dispose());
        return tcs.Task;
    }
    public static async Task<T> SuspendUntilNext<T>(this IReactiveChain<T> s)
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

        if (!eventEmitted)
            throw new SuspendInvocationException(delivered);

        tcs.TrySetResult(emittedEvent!);
        return await tcs.Task;
    }
    
    public static Task<T> SuspendUntilNextOfType<T>(this IReactiveChain<object> s)
        => s.OfType<T>().SuspendUntilNext();
    public static Task<T> SuspendUntilNextOfType<T>(this IReactiveChain<object> s, TimeSpan waitBeforeSuspension)
        => s.OfType<T>().SuspendUntilNext(waitBeforeSuspension);
    public static Task<T> SuspendUntilNextOfTypeOrTimeoutEventFired<T>(this IReactiveChain<object> s, string timeoutId, TimeSpan expiresIn)
        => s.OfType<T>().SuspendUntilNextOrTimeoutEventFired(timeoutId, expiresIn);
    public static Task<T> SuspendUntilNextOfTypeOrTimeoutEventFired<T>(this IReactiveChain<object> s, string timeoutId, DateTime expiresAt)
        => s.OfType<T>().SuspendUntilNextOrTimeoutEventFired(timeoutId, expiresAt);
    public static Task<T> SuspendUntilNextOrTimeoutEventFired<T>(this IReactiveChain<T> s, string timeoutId, TimeSpan expiresIn)
        => SuspendUntilNextOrTimeoutEventFired(s, timeoutId, expiresAt: DateTime.UtcNow.Add(expiresIn));
    public static async Task<T> SuspendUntilNextOrTimeoutEventFired<T>(this IReactiveChain<T> s, string timeoutId, DateTime expiresAt)
    {
        var tcs = new TaskCompletionSource<T>();
        
        ISubscription? subscription = null;
        ISubscription? timeoutSubscription = null;
        
        subscription = s.Subscribe(
            onNext: t =>
            {
                // ReSharper disable once AccessToModifiedClosure
                subscription?.Dispose();
                // ReSharper disable once AccessToModifiedClosure
                timeoutSubscription?.Dispose();

                tcs.TrySetResult(t);
            },
            onCompletion: () => { },
            onError: e => tcs.TrySetException(e)
        );

        timeoutSubscription = subscription
            .Source
            .OfType<TimeoutEvent>()
            .Where(t => t.TimeoutId == timeoutId)
            .Subscribe(
                onNext: _ =>
                {
                    subscription.Dispose();
                    // ReSharper disable once AccessToModifiedClosure
                    timeoutSubscription?.Dispose();
                    
                    tcs.TrySetException(new TimeoutException("Event was not emitted within timeout"));
                },
                onCompletion: () => {},
                onError: _ => {},
                subscription.SubscriptionGroupId
            );
        
        var delivered = subscription.DeliverExisting();

        if (tcs.Task.IsCompleted) return await tcs.Task;
        
        await subscription.TimeoutProvider.RegisterTimeout(timeoutId, expiresAt);
        throw new SuspendInvocationException(delivered);
    }

    public static async Task SuspendUntil(this EventSource s, DateTime resumeAt, string timeoutId)
    {
        var subscription = s
            .OfType<TimeoutEvent>()
            .Where(t => t.TimeoutId == timeoutId)
            .Subscribe(
                onNext: _ => {},
                onCompletion: () => {},
                onError: _ => {}
            );
        var delivered = subscription.DeliverExisting();
        if (delivered > 0)
            return;

        await subscription.TimeoutProvider.RegisterTimeout(timeoutId, resumeAt);
        throw new SuspendInvocationException(delivered);
    }

    public static Task SuspendFor(this EventSource s, TimeSpan resumeAfter, string timeoutId)
        => s.SuspendUntil(DateTime.UtcNow.Add(resumeAfter), timeoutId);

    public static Task Completion<T>(this IReactiveChain<T> s)
    {
        var tcs = new TaskCompletionSource();
        var subscription = s.Subscribe(
            onNext: _ => { },
            onCompletion: () => tcs.TrySetResult(),
            onError: e => tcs.TrySetException(e)
        );
        
        subscription.DeliverExistingAndFuture();
        tcs.Task.ContinueWith(_ => subscription.Dispose(), TaskContinuationOptions.ExecuteSynchronously);
        return tcs.Task;
    }
    
    #endregion
}