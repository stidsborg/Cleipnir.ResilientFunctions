using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public class InnerScheduled<TResult>(
    StoredType storedType,
    List<FlowId> scheduledIds,
    Workflow? parentWorkflow,
    ISerializer serializer,
    ResultBusyWaiter<TResult> resultBusyWaiter,
    Task<TResult>? task = null)
{
    public async Task<IReadOnlyList<TResult>> Completion(TimeSpan? timeout = null, bool allowPostponedAndSuspended = true)
    {
        if (task != null)
        {
            try
            {
                var result = await task;
                return new List<TResult>(1) { result };
            }
            catch
            {
                // fall back to normal logic
            }
        }

        return parentWorkflow == null
            ? await DetachedScheduled(timeout, allowPostponedAndSuspended)
            : await AttachedScheduled(parentWorkflow, timeout);
    }
    
    public Scheduled ToScheduledWithoutResult() => Scheduled.CreateFromInnerScheduled(this);
    public Scheduled<TResult> ToScheduledWithResult() => Scheduled<TResult>.CreateFromInnerScheduled(this);
    public BulkScheduled ToScheduledWithoutResults() => BulkScheduled.CreateFromInnerScheduled(this);
    public BulkScheduled<TResult> ToScheduledWithResults() => BulkScheduled<TResult>.CreateFromInnerScheduled(this);

    private async Task<IReadOnlyList<TResult>> DetachedScheduled(TimeSpan? timeout, bool allowPostponedAndSuspended = true)
    {
        timeout ??= TimeSpan.FromSeconds(10);
        var stopWatch = Stopwatch.StartNew();

        var results = new List<TResult>(scheduledIds.Count);
        foreach (var scheduledId in scheduledIds)
        {
            var timeLeft = timeout - stopWatch.Elapsed;
            if (timeLeft < TimeSpan.Zero)
                throw new TimeoutException();

            var storedId = scheduledId.ToStoredId(storedType);
            var result = await resultBusyWaiter.WaitForFunctionResult(scheduledId, storedId, allowPostponedAndSuspended, timeLeft);
            results.Add(result);
        }

        return results;
    }

    private async Task<IReadOnlyList<TResult>> AttachedScheduled(Workflow parent, TimeSpan? timeout)
    {
        return await parent.Effect.Capture(async () =>
        {
            var workTask = WaitForCompletions();
            var winner = await Task.WhenAny(workTask, Task.Delay(timeout ?? Timeout.InfiniteTimeSpan));
            if (winner != workTask)
                throw new TimeoutException();
            return await workTask;
        });

        async Task<IReadOnlyList<TResult>> WaitForCompletions()
        {
            var completedFlows = new List<FlowCompleted>();
            foreach (var _ in scheduledIds)
            {
                FlowCompleted completed;
                if (timeout == null)
                {
                    completed = await parent.Message<FlowCompleted>(filter: c => scheduledIds.Contains(c.Id));
                }
                else
                {
                    var maybe = await parent.Message<FlowCompleted>(
                        filter: c => scheduledIds.Contains(c.Id),
                        waitFor: timeout.Value
                    );
                    if (maybe == null)
                        throw new TimeoutException();
                    completed = maybe;
                }
                completedFlows.Add(completed);
            }

            var failed = completedFlows.FirstOrDefault(fc => fc.Failed);
            if (failed != null)
                throw new InvalidOperationException($"Child-flow '{failed.Id}' failed");

            var results = completedFlows.Select(fc =>
                new
                {
                    FlowId = fc.Id,
                    Result = fc.Result == null
                        ? default!
                        : (TResult)serializer.Deserialize(fc.Result, typeof(TResult))
                }

            ).ToDictionary(a => a.FlowId, a => a.Result);

            return scheduledIds.Select(id => results[id]).ToList();
        }
    }
}

public class Scheduled(Func<TimeSpan?, Task> completion)
{
    public async Task Completion(TimeSpan? timeout = null)
    {
        await completion(timeout);
    }

    internal static Scheduled CreateFromInnerScheduled<TResult>(InnerScheduled<TResult> inner)
        => new(timeout => inner.Completion(timeout));
}

public class Scheduled<TResult>(Func<TimeSpan?, Task<TResult>> completion)
{
    public async Task<TResult> Completion(TimeSpan? timeout = null) => await completion(timeout);

    internal static Scheduled<TResult> CreateFromInnerScheduled(InnerScheduled<TResult> inner)
        => new(async timeout => (await inner.Completion(timeout)).First());
}

public class BulkScheduled(Func<TimeSpan?, Task> completion)
{
    public async Task Completion(TimeSpan? timeout = null)
    {
        await completion(timeout);
    }

    internal static BulkScheduled CreateFromInnerScheduled<TResult>(InnerScheduled<TResult> inner) => new(timeout => inner.Completion(timeout));
}

public class BulkScheduled<TResult>(Func<TimeSpan?, Task<IReadOnlyList<TResult>>> completion)
{
    public async Task<IReadOnlyList<TResult>> Completion(TimeSpan? timeout = null) => await completion(timeout);

    internal static BulkScheduled<TResult> CreateFromInnerScheduled(InnerScheduled<TResult> inner) => new(timeout => inner.Completion(timeout));
}

public static class ScheduledExtensions
{
    public static async Task<IReadOnlyList<TResult>> Completion<TResult>(this Task<BulkScheduled<TResult>> scheduledTask, TimeSpan? timeout = null)
        => await (await scheduledTask).Completion(timeout);

    public static async Task Completion(this Task<BulkScheduled> scheduledTask, TimeSpan? timeout = null)
        => await (await scheduledTask).Completion(timeout);

    public static async Task<TResult> Completion<TResult>(this Task<Scheduled<TResult>> scheduledTask, TimeSpan? timeout = null)
        => await (await scheduledTask).Completion(timeout);

    public static async Task Completion(this Task<Scheduled> scheduledTask, TimeSpan? timeout = null)
        => await (await scheduledTask).Completion(timeout);
}