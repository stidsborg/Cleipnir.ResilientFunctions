using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;

namespace Cleipnir.ResilientFunctions.Domain;

public static class EffectExtensions
{
    public static Task CaptureEach<T>(this IEnumerable<T> elms, Func<T, Task> handler, string? alias = null)
    {
        var effect = CurrentFlow.Workflow?.Effect;
        if (effect == null)
            throw new InvalidOperationException("Must capture inside executing flow");

        return effect.ForEach(elms, handler, alias);
    }
    
    public static Task<TSeed> CaptureAggregate<T, TSeed>(
        this IEnumerable<T> elms,
        TSeed seed,
        Func<T, TSeed, Task<TSeed>> handler,
        string? alias = null)
    {
        var effect = CurrentFlow.Workflow?.Effect;
        if (effect == null)
            throw new InvalidOperationException("Must capture inside executing flow");

        return effect.AggregateEach(elms, seed, handler, alias);
    }
}