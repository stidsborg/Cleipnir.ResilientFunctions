using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Messaging;
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
                return (next, notify, complete, _) =>
                {
                    if (!predicate(next))
                        notify(next);
                    else
                        complete();
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
    
    public static IReactiveChain<T> SkipUntil<T>(this IReactiveChain<T> s, Func<T, bool> predicate)
    {
        return s.WithOperator<T, T>(
            () =>
            {
                var emitting = false;
                return (next, notify, _, _) =>
                {
                    emitting = emitting || predicate(next);

                    if (emitting)
                        notify(next);
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
    public static List<T> Existing<T>(this IReactiveChain<T> s)
    {
        var tcs = new TaskCompletionSource<List<T>>();
        var list = new List<T>();
        using var subscription = s.Subscribe(
            onNext: t => list.Add(t),
            onCompletion: () => tcs.TrySetResult(list),
            onError: e => tcs.TrySetException(e)
        );
        
        subscription.DeliverExisting();

        return list;
    }
    
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
                    tcs.TrySetException(NoResultException.NewInstance);
                else
                    tcs.TrySetResult(emittedEvent!);
            },
            onError: e => tcs.TrySetException(e)
        );
        
        subscription.DeliverExistingAndFuture();

        tcs.Task.ContinueWith(_ => subscription.Dispose(), TaskContinuationOptions.ExecuteSynchronously);
            
        return tcs.Task;
    }
    
    public static Task<List<T>> Lasts<T>(this IReactiveChain<T> s)
    {
        var tcs = new TaskCompletionSource<List<T>>();
        var list = new List<T>();
        var subscription = s.Subscribe(
            onNext: t => list.Add(t),
            onCompletion: () => tcs.TrySetResult(list),
            onError: e => tcs.TrySetException(e)
        );
        subscription.DeliverExistingAndFuture();

        tcs.Task.ContinueWith(_ => subscription.Dispose(), TaskContinuationOptions.ExecuteSynchronously);
        
        return tcs.Task;
    }
    
    public static Task<List<T>> Lasts<T>(this IReactiveChain<T> s, int count)
    {
        var tcs = new TaskCompletionSource<List<T>>();
        var emits = new LinkedList<T>();
        
        var subscription = s.Subscribe(
            onNext: t =>
            {
                emits.AddLast(t);
                if (emits.Count > count)
                    emits.RemoveFirst();
            },
            onCompletion: () => tcs.TrySetResult(emits.ToList()),
            onError: e => tcs.TrySetException(e)
        );
        
        subscription.DeliverExistingAndFuture();

        tcs.Task.ContinueWith(_ => subscription.Dispose(), TaskContinuationOptions.ExecuteSynchronously);
            
        return tcs.Task;
    }
    
    public static TryLastOutcome TryLast<T>(this IReactiveChain<T> s, out T? last)
        => TryLast(s, out last, out _);
    public static TryLastOutcome TryLast<T>(this IReactiveChain<T> s, out T? last, out int totalEventSourceCount) 
    {
        var (hasValue, value, _, _, hasCompleted, _, _, emittedFromSource) = PullLast(s);

        if (hasValue)
        {
            last = value;
            totalEventSourceCount = emittedFromSource;
            return TryLastOutcome.StreamCompletedWithValue;
        }

        last = default;
        totalEventSourceCount = emittedFromSource;
        return hasCompleted 
            ? TryLastOutcome.SteamCompletedWithoutValue 
            : TryLastOutcome.NonCompletedStream;
    }
    
    public static bool TryCompletes<T>(this IReactiveChain<T> s)
        => TryCompletes(s, out _);
    public static bool TryCompletes<T>(this IReactiveChain<T> s, out int totalEventSourceCount)
    {
        var completed = false;
        using var subscription = s.Subscribe(
            onNext: _ => { },
            onCompletion: () => completed = true,
            onError: _ => { }
        );

        totalEventSourceCount = subscription.DeliverExisting();
        
        return completed;
    }
    
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
    
    public static async Task<T> SuspendUntilNext<T>(this IReactiveChain<T> s, TimeSpan waitBeforeSuspension)
    {
        var (hasValue, value, hasException, exception, hasCompleted, _, _, _) = PullNext(s);
        if (hasException)
            throw exception!;
        if (hasValue)
            return value!;
        if (hasCompleted)
            throw NoResultException.NewInstance;
        
        var tcs = new TaskCompletionSource();
        using var subscription = s.Subscribe(
            onNext: _ => tcs.TrySetResult(),
            onCompletion: () => tcs.TrySetResult(),
            onError: _ => tcs.TrySetResult()
        );
        subscription.DeliverExistingAndFuture();

        var cts = new CancellationTokenSource();
        var delayTask = Task.Delay(waitBeforeSuspension, cts.Token);

        await Task.WhenAny(tcs.Task, delayTask);

        if (!delayTask.IsCompleted)
            cts.Cancel();

        return await SuspendUntilNext(s);
    }
    public static Task<T> SuspendUntilNext<T>(this IReactiveChain<T> s)
    {
        var tcs = new TaskCompletionSource<T>();
        var (hasValue, value, hasException, exception, hasCompleted, _, _, emittedFromSource) = PullNext(s);
        
        if (hasValue)
            tcs.SetResult(value!);
        else if (hasException)
            tcs.SetException(exception!);
        else if (hasCompleted)
            tcs.SetException(NoResultException.NewInstance);
        else
            tcs.SetException(new SuspendInvocationException(emittedFromSource));

        return tcs.Task;
    }
    
    public static Task<T> SuspendUntilNextOfType<T>(this IReactiveChain<object> s)
        => s.OfType<T>().SuspendUntilNext();
    public static Task<T> SuspendUntilNextOfType<T>(this IReactiveChain<object> s, TimeSpan waitBeforeSuspension)
        => s.OfType<T>().SuspendUntilNext(waitBeforeSuspension);
    
    public static Task<TimeoutOption<T>> SuspendUntilNext<T>(this IReactiveChain<T> s, string timeoutEventId, TimeSpan expiresIn)
        => SuspendUntilNext(s, timeoutEventId, expiresAt: DateTime.UtcNow.Add(expiresIn));
    public static async Task<TimeoutOption<T>> SuspendUntilNext<T>(this IReactiveChain<T> s, string timeoutEventId, DateTime expiresAt)
    {
        var (hasValue, value, hasException, exception, _, timeoutOccured, timeoutProvider, emittedFromSource) = PullNext(s, timeoutEventId);

        if (timeoutOccured)
            return new TimeoutOption<T>(TimedOut: true, Value: default);
        if (hasValue)
            return new TimeoutOption<T>(TimedOut: false, value);
        if (hasException)
            throw exception!;
        
        await timeoutProvider.RegisterTimeout(timeoutEventId, expiresAt);
        
        throw new SuspendInvocationException(emittedFromSource);
    }

    public static async Task SuspendUntil(this EventSource s, string timeoutEventId, DateTime resumeAt)
    {
        var subscription = s
            .OfType<TimeoutEvent>()
            .Where(t => t.TimeoutId == timeoutEventId)
            .Subscribe(
                onNext: _ => {},
                onCompletion: () => {},
                onError: _ => {}
            );
        var delivered = subscription.DeliverExisting();
        if (delivered > 0)
            return;

        await subscription.TimeoutProvider.RegisterTimeout(timeoutEventId, resumeAt);
        throw new SuspendInvocationException(delivered);
    }

    public static Task SuspendFor(this EventSource s, string timeoutEventId, TimeSpan resumeAfter)
        => s.SuspendUntil(timeoutEventId, DateTime.UtcNow.Add(resumeAfter));
    
    /* SUSPEND_UNTIL_COMPLETION */
    
    public static Task SuspendUntilCompletion<T>(this IReactiveChain<T> s)
    {
        var (_, _, hasException, exception, hasCompleted, _, _, emittedFromSource) = PullLast(s);
        var tcs = new TaskCompletionSource();
        if (hasException)
            tcs.SetException(exception!);
        else if (hasCompleted)
            tcs.SetResult();
        else
            tcs.SetException(new SuspendInvocationException(emittedFromSource));
        
        return tcs.Task;
    }

    public static async Task SuspendUntilCompletion<T>(this IReactiveChain<T> s, TimeSpan waitBeforeSuspension)
    {
        var (_, _, hasException, exception, hasCompleted, _, _, _) = PullLast(s);
        if (hasCompleted)
            return;
        if (hasException)
            throw exception!;

        var tcs = new TaskCompletionSource();
        using var cts = new CancellationTokenSource();
        using var delayTask = Task.Delay(waitBeforeSuspension, cts.Token);

        using var subscription = s.Subscribe(
            onNext: _ => { },
            onCompletion: () => tcs.TrySetResult(),
            onError: e => tcs.SetException(e)
        );
        subscription.DeliverExistingAndFuture();
        
        await Task.WhenAny(tcs.Task, delayTask);
        
        cts.Cancel();
        await SuspendUntilCompletion(s); //suspend or throw potential exception
    }

    public static async Task<TimeoutOption> SuspendUntilCompletion<T>(this IReactiveChain<T> s, string timeoutEventId, DateTime timeoutAt)
    {
        var (hasValue, _, hasException, exception, _, timeoutOccured, timeoutProvider, emittedFromSource) = PullLast(s, timeoutEventId);

        if (timeoutOccured)
            return new TimeoutOption(TimedOut: true);
        if (hasValue)
            return new TimeoutOption(TimedOut: false);
        if (hasException)
            throw exception!;
        
        await timeoutProvider.RegisterTimeout(timeoutEventId, timeoutAt);
        
        throw new SuspendInvocationException(emittedFromSource);
    }

    public static Task<TimeoutOption> SuspendUntilCompletion<T>(this IReactiveChain<T> s, string timeoutEventId, TimeSpan timeoutIn) 
        => SuspendUntilCompletion(s, timeoutEventId, DateTime.UtcNow.Add(timeoutIn));
    
    
    /* SUSPEND_UNTIL_LAST */
    public static Task<T> SuspendUntilLast<T>(this IReactiveChain<T> s)
    {
        var tcs = new TaskCompletionSource<T>();
        var (hasValue, value, hasException, exception, hasCompleted, _, _, emittedFromSource) = PullLast(s);
        if (hasException)
            tcs.SetException(exception!);
        else if (hasValue && hasCompleted)
            tcs.SetResult(value!);
        else if (hasCompleted)
            tcs.SetException(NoResultException.NewInstance);
        else
            tcs.SetException(new SuspendInvocationException(emittedFromSource));

        return tcs.Task;
    }
    
    public static async Task<T> SuspendUntilLast<T>(this IReactiveChain<T> s, TimeSpan waitBeforeSuspension)
    {
        var (hasValue, value, hasException, exception, hasCompleted, _, _, _) = PullLast(s);
        if (hasException)
            throw exception!;
        if (hasCompleted && !hasValue)
            return await SuspendUntilLast(s);
        if (hasCompleted && hasValue)
            return value!;

        var tcs = new TaskCompletionSource();
        using var cts = new CancellationTokenSource();
        using var delayTask = Task.Delay(waitBeforeSuspension, cts.Token);

        using var subscription = s.Subscribe(
            onNext: _ => { },
            onCompletion: () => tcs.TrySetResult(),
            onError: e => tcs.SetException(e)
        );
        
        subscription.DeliverExistingAndFuture();
        
        await Task.WhenAny(tcs.Task, delayTask);
        
        cts.Cancel();
        return await SuspendUntilLast(s); //suspend or throw potential exception
    }
    
    public static async Task<TimeoutOption<T>> SuspendUntilLast<T>(this IReactiveChain<T> s, string timeoutEventId, DateTime timeoutAt)
    {
        var (hasValue, value, hasException, exception, hasCompleted, timeoutOccured, timeoutProvider, emittedFromSource) = PullLast(s, timeoutEventId);

        if (hasException)
            throw exception!;
        if (timeoutOccured)
            return new TimeoutOption<T>(TimedOut: true, Value: default);
        if (hasValue && hasCompleted)
            return new TimeoutOption<T>(TimedOut: false, Value: value!);
        if (!hasValue && hasCompleted)
            throw NoResultException.NewInstance;
        
        await timeoutProvider.RegisterTimeout(timeoutEventId, timeoutAt);
        
        throw new SuspendInvocationException(emittedFromSource);
    }

    public static Task<TimeoutOption<T>> SuspendUntilLast<T>(this IReactiveChain<T> s, string timeoutEventId, TimeSpan timeoutIn) 
        => SuspendUntilLast(s, timeoutEventId, DateTime.UtcNow.Add(timeoutIn));
    
    /* PULL HELPER METHODS */
    private static PullResult<T> PullNext<T>(this IReactiveChain<T> s, string? timeoutEventId = null)
    {
        var voe = new PullResult<T>(
            HasValue: false,
            Value: default,
            HasException: false,
            Exception: null,
            HasCompleted: false,
            TimeoutOccured: false,
            TimeoutProvider: default!,
            EmittedFromSource: 0
        );
        
        using var subscription = s.Subscribe(
            onNext: t =>
            {
                if (voe is { HasValue: false, HasException: false, TimeoutOccured: false })
                    voe = voe with { HasValue = true, Value = t };
            },
            onCompletion: () => voe = voe with { HasCompleted = true },
            onError: exception =>
            {
                if (voe is { HasValue: false, HasException: false, TimeoutOccured: false })
                    voe = voe with { HasException = true, Exception = exception };
            }
        );

        ISubscription? timeoutSubscription = null;
        if (timeoutEventId != null)
            timeoutSubscription = subscription.Source
                .OfType<TimeoutEvent>()
                .Where(t => t.TimeoutId == timeoutEventId)
                .Take(1)
                .Subscribe(
                    onNext: t =>
                    {
                        if (voe is { HasValue: false, HasException: false, TimeoutOccured: false })
                            voe = voe with { TimeoutOccured = true };
                    },
                    onCompletion: () => { },
                    onError: _ => { },
                    subscriptionGroupId: subscription.SubscriptionGroupId
                );
        
        var emittedFromSource = subscription.DeliverExisting();

        timeoutSubscription?.Dispose();
        
        return voe with { EmittedFromSource = emittedFromSource, TimeoutProvider = subscription.TimeoutProvider };
    }
    
    private static PullResult<T> PullLast<T>(this IReactiveChain<T> s, string? timeoutEventId = null)
    {
        T? latestEmitted = default;
        var hasEmitted = false;
        
        var voe = new PullResult<T>(
            HasValue: false,
            Value: default,
            HasException: false,
            Exception: null,
            HasCompleted: false,
            TimeoutOccured: false,
            TimeoutProvider: default!,
            EmittedFromSource: 0
        );
        
        using var subscription = s.Subscribe(
            onNext: t =>
            {
                hasEmitted = true;
                latestEmitted = t;
            },
            onCompletion: () =>
            {
                if (!voe.TimeoutOccured)
                    voe = voe with { HasCompleted = true };
            },
            onError: exception =>
            {
                if (!voe.TimeoutOccured)
                    voe = voe with { HasException = true, Exception = exception };
            });

        ISubscription? timeoutSubscription = null;
        if (timeoutEventId != null)
            timeoutSubscription = subscription
                .Source
                .OfType<TimeoutEvent>()
                .Where(t => t.TimeoutId == timeoutEventId)
                .Take(1)
                .Subscribe(
                    onNext: _ =>
                    {
                        if (voe is { HasCompleted: false, HasException: false })
                            voe = voe with { TimeoutOccured = true };
                    },
                    onError: _ => {},
                    onCompletion: () => {},
                    subscriptionGroupId: subscription.SubscriptionGroupId
                );
        
        var emittedFromSource = subscription.DeliverExisting();
        timeoutSubscription?.Dispose();

        if (voe is { HasException: false, TimeoutOccured: false } && hasEmitted)
            voe = voe with { HasValue = true, Value = latestEmitted };
        
        return voe with { EmittedFromSource = emittedFromSource, TimeoutProvider = subscription.TimeoutProvider };
    }

    private record struct PullResult<T>(bool HasValue, T? Value, bool HasException, Exception? Exception, bool HasCompleted, bool TimeoutOccured, ITimeoutProvider TimeoutProvider, int EmittedFromSource);
    
    #endregion
}