using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public class InnerScheduled<TResult>(
    StoredType storedType,
    List<FlowId> scheduledIds, 
    Workflow? parentWorkflow, 
    ISerializer serializer,
    ResultBusyWaiter<TResult> resultBusyWaiter)
{
    public Task<IReadOnlyList<TResult>> Completion(TimeSpan? maxWait = null) 
        => parentWorkflow == null
            ? DetachedScheduled(maxWait)
            : AttachedScheduled(parentWorkflow, maxWait);
    
    public Scheduled ToScheduledWithoutResult() => Scheduled.CreateFromInnerScheduled(this);
    public Scheduled<TResult> ToScheduledWithResult() => Scheduled<TResult>.CreateFromInnerScheduled(this);
    public BulkScheduled ToScheduledWithoutResults() => BulkScheduled.CreateFromInnerScheduled(this);
    public BulkScheduled<TResult> ToScheduledWithResults() => BulkScheduled<TResult>.CreateFromInnerScheduled(this);

    private async Task<IReadOnlyList<TResult>> DetachedScheduled(TimeSpan? maxWait)
    {
        maxWait ??= TimeSpan.FromSeconds(10);
        var stopWatch = Stopwatch.StartNew();
        
        var results = new List<TResult>(scheduledIds.Count);
        foreach (var scheduledId in scheduledIds)
        {
            var timeLeft = maxWait - stopWatch.Elapsed;
            if (timeLeft < TimeSpan.Zero)
                throw new TimeoutException();

            var storedId = scheduledId.ToStoredId(storedType);
            var result = await resultBusyWaiter.WaitForFunctionResult(scheduledId, storedId, allowPostponedAndSuspended: true, timeLeft);
            results.Add(result);
        }

        return results;
    }
    
    private async Task<IReadOnlyList<TResult>> AttachedScheduled(Workflow parent, TimeSpan? maxWait)
    {
        var completedFlows = await parent
            .Messages
            .OfType<FlowCompleted>()
            .Where(fc => scheduledIds.Contains(fc.Id))
            .Take(scheduledIds.Count)
            .Completion(maxWait);

        var failed = completedFlows.FirstOrDefault(fc => fc.Failed);
        if (failed != null)
            throw new InvalidOperationException($"Child-flow '{failed.Id}' failed");
        
        var results = completedFlows.Select(fc =>
            new
            {
                FlowId = fc.Id,
                Result = fc.Result == null
                    ? default!
                    : serializer.Deserialize<TResult>(fc.Result)    
            }
                
        ).ToDictionary(a => a.FlowId, a => a.Result);

        return scheduledIds.Select(id => results[id]).ToList();
    }
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