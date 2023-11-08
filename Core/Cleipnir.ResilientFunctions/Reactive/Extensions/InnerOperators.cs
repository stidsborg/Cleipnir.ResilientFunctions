﻿using System;
using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Reactive.Operators;
using Cleipnir.ResilientFunctions.Reactive.Utilities;

namespace Cleipnir.ResilientFunctions.Reactive.Extensions;

public static class InnerOperators
{
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

    public static IReactiveChain<T> TakeUntilTimeout<T>(this IReactiveChain<T> s, string timeoutEventId, TimeSpan expiresIn, bool overwrite = false)
        => s.TakeUntilTimeout(timeoutEventId, expiresAt: DateTime.UtcNow.Add(expiresIn), overwrite);
    public static IReactiveChain<T> TakeUntilTimeout<T>(this IReactiveChain<T> s, string timeoutEventId, DateTime expiresAt, bool overwrite = false)
        => new TimeoutOperator<T>(s, timeoutEventId, expiresAt, overwrite, signalErrorOnTimeout: false);

    public static IReactiveChain<T> FailOnTimeout<T>(this IReactiveChain<T> s, string timeoutEventId, TimeSpan expiresIn, bool overwrite = false)
        => FailOnTimeout(s, timeoutEventId, expiresAt: DateTime.UtcNow.Add(expiresIn), overwrite);
    public static IReactiveChain<T> FailOnTimeout<T>(this IReactiveChain<T> s, string timeoutEventId, DateTime expiresAt, bool overwrite = false)
        => new TimeoutOperator<T>(s, timeoutEventId, expiresAt, overwrite, signalErrorOnTimeout: true);

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
}