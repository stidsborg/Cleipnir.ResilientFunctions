using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions;

public class InnerScheduled<TResult>(Func<TimeSpan?, Task<IReadOnlyList<TResult>>> completion)
{
    public static InnerScheduled<TResult> Failing { get; } = new(_ => throw new InvalidOperationException("Cannot wait for completion of detached flow"));
    
    public async Task<IReadOnlyList<TResult>> Completion(TimeSpan? maxWait = null) => await completion(maxWait);
    public Scheduled ToScheduledWithoutResult() => Scheduled.CreateFromInnerScheduled(this);
    public Scheduled<TResult> ToScheduledWithResult() => Scheduled<TResult>.CreateFromInnerScheduled(this);
    public BulkScheduled ToScheduledWithoutResults() => BulkScheduled.CreateFromInnerScheduled(this);
    public BulkScheduled<TResult> ToScheduledWithResults() => BulkScheduled<TResult>.CreateFromInnerScheduled(this);
}

public class Scheduled(Func<TimeSpan?, Task> completion)
{
    public async Task Completion(TimeSpan? maxWait = null)
    {
        await completion(maxWait);
    }

    internal static Scheduled CreateFromInnerScheduled<TResult>(InnerScheduled<TResult> inner) 
        => new(inner.Completion);
}

public class Scheduled<TResult>(Func<TimeSpan?, Task<TResult>> completion)
{
    public async Task<TResult> Completion(TimeSpan? maxWait = null) => await completion(maxWait);
    
    internal static Scheduled<TResult> CreateFromInnerScheduled(InnerScheduled<TResult> inner) 
        => new(async maxWait => (await inner.Completion(maxWait)).First());
}

public class BulkScheduled(Func<TimeSpan?, Task> completion)
{
    public async Task Completion(TimeSpan? maxWait = null)
    {
        await completion(maxWait);
    }
    
    internal static BulkScheduled CreateFromInnerScheduled<TResult>(InnerScheduled<TResult> inner) => new(inner.Completion);
}

public class BulkScheduled<TResult>(Func<TimeSpan?, Task<IReadOnlyList<TResult>>> completion)
{
    public async Task<IReadOnlyList<TResult>> Completion(TimeSpan? maxWait = null) => await completion(maxWait);
    
    internal static BulkScheduled<TResult> CreateFromInnerScheduled(InnerScheduled<TResult> inner) => new(inner.Completion);
}

public static class ScheduledExtensions
{
    public static async Task<IReadOnlyList<TResult>> Completion<TResult>(this Task<BulkScheduled<TResult>> scheduledTask, TimeSpan? maxWait = null) 
        => await (await scheduledTask).Completion(maxWait);
    
    public static async Task Completion(this Task<BulkScheduled> scheduledTask, TimeSpan? maxWait = null)    
        => await (await scheduledTask).Completion(maxWait);
    
    public static async Task<TResult> Completion<TResult>(this Task<Scheduled<TResult>> scheduledTask, TimeSpan? maxWait = null)
        => await (await scheduledTask).Completion(maxWait);
    
    public static async Task Completion(this Task<Scheduled> scheduledTask, TimeSpan? maxWait = null)
        => await (await scheduledTask).Completion(maxWait);
}