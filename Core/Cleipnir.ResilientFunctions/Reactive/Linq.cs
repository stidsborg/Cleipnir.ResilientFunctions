using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Reactive.Awaiter;

namespace Cleipnir.ResilientFunctions.Reactive;

public static class Linq
{
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

    public static IStream<TOut> WithOperator<TIn, TOut>(this IStream<TIn> inner, Func<Operator<TIn, TOut>> operatorFunc)
        => new CustomOperator<TIn, TOut>(inner, operatorFunc);
        
    public static IStream<TOut> WithOperator<TIn, TOut>(this IStream<TIn> inner, Operator<TIn, TOut> @operator)
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

    public static async Task<T> Next<T>(this IStream<T> s) => await s.Take(1).Last();
    public static Task<T> Last<T>(this IStream<T> s)
    {
        var tcs = new TaskCompletionSource<T>();
            var eventEmitted = false;
            var emittedEvent = default(T);
            
            s.Subscribe(
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
            
            return tcs.Task;
    }  
}