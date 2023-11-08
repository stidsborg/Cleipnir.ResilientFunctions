using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Utilities;

namespace Cleipnir.ResilientFunctions.Reactive.Extensions.Work;

public static class WorkExtensions
{
    public static async Task DoAtLeastOnce(this EventSource eventSource, string workId, Func<Task> work)
    {
        if (eventSource.Existing.OfType<WorkCompleted>().Any(workCompleted => workCompleted.WorkId == workId))
            return;
        
        await work();
        await eventSource.AppendEvent(new WorkCompleted(workId));
    }

    public static async Task DoAtMostOnce(this EventSource eventSource, string workId, Func<Task> work)
    {
        if (eventSource.Existing.OfType<WorkCompleted>().Any(workCompleted => workCompleted.WorkId == workId))
            return;
        if (eventSource.Existing.OfType<WorkStarted>().Any(workStarted => workStarted.WorkId == workId))
            throw new InvalidOperationException($"Work '{workId}' started but not completed previously");

        await eventSource.AppendEvent(new WorkStarted(workId));
        await work();
        await eventSource.AppendEvent(new WorkCompleted(workId));
    }
    
    public static async Task<TResult> DoAtLeastOnce<TResult>(this EventSource eventSource, string workId, Func<Task<TResult>> work)
    {
        var completedWork = eventSource
            .Existing
            .OfType<WorkCompleted>()
            .FirstOrDefault(workStarted => workStarted.WorkId == workId);

        if (completedWork != null)
            return ((WorkWithResultCompleted<TResult>)completedWork).Result;
        
        var result = await work();
        await eventSource.AppendEvent(new WorkWithResultCompleted<TResult>(workId, result));
        return result;
    }

    public static async Task<TResult> DoAtMostOnce<TResult>(this EventSource eventSource, string workId, Func<Task<TResult>> work)
    {
        var completedWork = eventSource
            .Existing
            .OfType<WorkCompleted>()
            .FirstOrDefault(workCompleted => workCompleted.WorkId == workId);
        
        if (completedWork != null)
            return ((WorkWithResultCompleted<TResult>) completedWork).Result;
        
        if (eventSource.Existing.OfType<WorkStarted>().Any(workStarted => workStarted.WorkId == workId))
            throw new InvalidOperationException($"Work '{workId}' started but not completed previously");   

        await eventSource.AppendEvent(new WorkStarted(workId));
        var result = await work();
        await eventSource.AppendEvent(new WorkWithResultCompleted<TResult>(workId, result));

        return result;
    }
}