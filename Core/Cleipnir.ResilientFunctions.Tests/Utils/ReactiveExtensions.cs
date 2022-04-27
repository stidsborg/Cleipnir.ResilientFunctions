using System;
using System.Reactive.Concurrency;
using System.Reactive.Linq;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class ReactiveExtensions
{
    public static IObservable<T> ObserveOnThreadPool<T>(this IObservable<T> o)
        => o.ObserveOn(ThreadPoolScheduler.Instance);
}