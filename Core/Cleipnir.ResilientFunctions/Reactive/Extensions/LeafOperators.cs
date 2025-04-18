﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Utilities;

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
    
    public static Task<List<T>> Completion<T>(this IReactiveChain<T> s, TimeSpan? maxWait = null)
        => s.ToList(maxWait);
    
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

    #region First
    public static Task<T> First<T>(this IReactiveChain<T> s, TimeSpan? maxWait = null)
        => FirstOrNone(s, maxWait)
            .SelectAsync(o => o.HasValue ? o.Value : throw new NoResultException());
    
    public static Task<T?> FirstOrDefault<T>(this IReactiveChain<T> s, TimeSpan? maxWait = null)
        => FirstOrNone(s, maxWait)
            .SelectAsync(o => o.HasValue ? o.Value : default);
    public static Task<Option<T>> FirstOrNone<T>(this IReactiveChain<T> s, TimeSpan? maxWait = null)
        => Firsts(s, count: 1, maxWait)
            .SelectAsync(
                l => l.Any() ? Option.Create(l.First()) : Option<T>.NoValue
            );
    
    public static Task<EitherOrNone<T1, T2>> FirstOrNone<T1, T2>(this IReactiveChain<Either<T1, T2>> s, TimeSpan? maxWait = null)
        => Firsts(s, count: 1, maxWait)
            .SelectAsync(
                either => either.Any() 
                    ? EitherOrNone<T1, T2>.CreateFromEither(either.First()) 
                    : EitherOrNone<T1, T2>.CreateNone()
            );

    public static Task<EitherOrNone<T1, T2, T3>> FirstOrNone<T1, T2, T3>(this IReactiveChain<Either<T1, T2, T3>> s, TimeSpan? maxWait = null)
        => Firsts(s, count: 1, maxWait)
            .SelectAsync(
                either => either.Any()
                    ? EitherOrNone<T1, T2, T3>.CreateFromEither(either.First())
                    : EitherOrNone<T1, T2, T3>.CreateNone()
            );
    
    public static Task<T> FirstOfType<T>(this IReactiveChain<object> s, TimeSpan? maxWait = null)
        => s.OfType<T>().First(maxWait);
    public static Task<Option<T>> FirstOfType<T>(this Messages messages, string timeoutId, DateTime expiresAt, TimeSpan? maxWait = null)
        => messages.TakeUntilTimeout(timeoutId, expiresAt).OfType<T>().FirstOrNone(maxWait);
    public static Task<Option<T>> FirstOfType<T>(this Messages messages, string timeoutId, TimeSpan expiresIn, TimeSpan? maxWait = null)
        => messages.TakeUntilTimeout(timeoutId, expiresIn).OfType<T>().FirstOrNone(maxWait);
    
    public static Task<Either<T1, T2>> FirstOfTypes<T1, T2>(this IReactiveChain<object> s, TimeSpan? maxWait = null)
        => s.OfTypes<T1, T2>().First(maxWait);
    public static Task<EitherOrNone<T1, T2>> FirstOfTypes<T1, T2>(this Messages messages, string timeoutId, DateTime expiresAt, TimeSpan? maxWait = null)
        => messages.TakeUntilTimeout(timeoutId, expiresAt).OfTypes<T1, T2>().FirstOrNone(maxWait);
    public static Task<EitherOrNone<T1, T2>> FirstOfTypes<T1, T2>(this Messages messages, string timeoutId, TimeSpan expiresIn, TimeSpan? maxWait = null)
        => messages.TakeUntilTimeout(timeoutId, expiresIn).OfTypes<T1, T2>().FirstOrNone(maxWait);
    public static Task<EitherOrNone<T1, T2>> FirstOfTypes<T1, T2>(this Messages messages, DateTime expiresAt, TimeSpan? maxWait = null)
        => messages.TakeUntilTimeout(expiresAt).OfTypes<T1, T2>().FirstOrNone(maxWait);
    public static Task<EitherOrNone<T1, T2>> FirstOfTypes<T1, T2>(this Messages messages, TimeSpan expiresIn, TimeSpan? maxWait = null)
        => messages.TakeUntilTimeout(expiresIn).OfTypes<T1, T2>().FirstOrNone(maxWait);
    
    public static Task<EitherOrNone<T1, T2, T3>> FirstOfTypes<T1, T2, T3>(this Messages messages, string timeoutId, DateTime expiresAt, TimeSpan? maxWait = null)
        => messages.TakeUntilTimeout(timeoutId, expiresAt).OfTypes<T1, T2, T3>().FirstOrNone(maxWait);
    public static Task<EitherOrNone<T1, T2, T3>> FirstOfTypes<T1, T2, T3>(this Messages messages, string timeoutId, TimeSpan expiresIn, TimeSpan? maxWait = null)
        => messages.TakeUntilTimeout(timeoutId, expiresIn).OfTypes<T1, T2, T3>().FirstOrNone(maxWait);
    public static Task<EitherOrNone<T1, T2, T3>> FirstOfTypes<T1, T2, T3>(this Messages messages, DateTime expiresAt, TimeSpan? maxWait = null)
        => messages.TakeUntilTimeout(expiresAt).OfTypes<T1, T2, T3>().FirstOrNone(maxWait);
    public static Task<EitherOrNone<T1, T2, T3>> FirstOfTypes<T1, T2, T3>(this Messages messages, TimeSpan expiresIn, TimeSpan? maxWait = null)
        => messages.TakeUntilTimeout(expiresIn).OfTypes<T1, T2, T3>().FirstOrNone(maxWait);

    public static Task<T> FirstOf<T>(this IReactiveChain<object> s, TimeSpan? maxWait = null) 
        => s.FirstOfType<T>(maxWait);
    
    public static Task<List<T>> Firsts<T>(this IReactiveChain<T> s, int count, TimeSpan? maxWait = null)
        => s.Take(count).ToList(maxWait);
    
    #endregion
    
    #region Last
    
    public static Task<List<T>> Lasts<T>(this IReactiveChain<T> s, int count, TimeSpan? maxWait = null) 
        => s.ToList(maxWait).SelectAsync(l => l.TakeLast(count).ToList());
    
    public static Task<T> Last<T>(this IReactiveChain<T> s, TimeSpan? maxWait = null)
        => LastOrNone(s, maxWait)
            .SelectAsync(o => o.HasValue ? o.Value : throw new NoResultException());
    public static Task<T?> LastOrDefault<T>(this IReactiveChain<T> s)
        => LastOrNone(s)
            .SelectAsync(o => o.HasValue ? o.Value : default);
    public static Task<Option<T>> LastOrNone<T>(this IReactiveChain<T> s, TimeSpan? maxWait = null)
        => Lasts(s, count: 1, maxWait)
            .SelectAsync(l =>
                l.Any()
                    ? Option.Create(l.First())
                    : Option<T>.NoValue
            );

    public static Task<T> LastOfType<T>(this IReactiveChain<object> s)
        => s.OfType<T>().Last();
    public static Task<T> LastOf<T>(this IReactiveChain<object> s) => s.LastOfType<T>();
    
    public static Task<Option<T>> LastOfType<T>(this Messages messages, string timeoutId, DateTime expiresAt)
        => messages.TakeUntilTimeout(timeoutId, expiresAt).OfType<T>().LastOrNone();
    public static Task<Option<T>> LastOfType<T>(this Messages messages, string timeoutId, TimeSpan expiresIn)
        => messages.TakeUntilTimeout(timeoutId, expiresIn).OfType<T>().LastOrNone();
    
    #endregion

    #region Suspend

    public static async Task SuspendUntil(this Messages s, string timeoutEventId, DateTime resumeAt)
    {
        var timeoutEmitted = false;
        var effectId = EffectId.CreateWithCurrentContext(timeoutEventId, EffectType.Timeout);
        var subscription = s
            .OfType<TimeoutEvent>()
            .Where(t => t.TimeoutId == effectId)
            .Take(1)
            .Subscribe(
                onNext: _ => timeoutEmitted = true,
                onCompletion: () => { },
                onError: _ => { }
            );
        await subscription.Initialize();
        
        subscription.PushMessages();
        
        if (timeoutEmitted)
            return;

        await subscription.RegisteredTimeouts.RegisterTimeout(effectId, resumeAt);
        throw new SuspendInvocationException();
    }

    public static Task SuspendFor(this Messages s, string timeoutEventId, TimeSpan resumeAfter)
        => s.SuspendUntil(timeoutEventId, resumeAt: s.UtcNow().Add(resumeAfter));

    #endregion
}