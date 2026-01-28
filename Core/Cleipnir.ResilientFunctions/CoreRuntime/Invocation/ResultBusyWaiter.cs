using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class ResultBusyWaiter<TResult>(IFunctionStore functionStore, ISerializer serializer)
{
    public async Task<TResult> WaitForFunctionResult(FlowId flowId, StoredId storedId, bool allowPostponedAndSuspended, TimeSpan? maxWait)
    {
        var stopWatch = Stopwatch.StartNew();
        while (true)
        {
            if (maxWait.HasValue && stopWatch.Elapsed > maxWait.Value)
                throw new TimeoutException();

            var storedFunction = await functionStore.GetFunction(storedId);
            if (storedFunction == null)
                throw UnexpectedStateException.NotFound(storedId);

            switch (storedFunction.Status)
            {
                case Status.Executing:
                    await Task.Delay(250);
                    continue;
                case Status.Succeeded:
                    var results = await functionStore.GetResults([storedId]);
                    var result = results.TryGetValue(storedId, out var resultBytes) ? resultBytes : null;
                    return
                        result == null
                            ? default!
                            : (TResult)serializer.Deserialize(result, typeof(TResult));
                case Status.Failed:
                    throw FatalWorkflowException.Create(flowId, storedFunction.Exception!);
                case Status.Postponed:
                    if (allowPostponedAndSuspended) { await Task.Delay(250); continue;}
                    throw new InvocationPostponedException(
                        flowId,
                        postponedUntil: new DateTime(storedFunction.Expires, DateTimeKind.Utc)
                    );
                case Status.Suspended:
                    if (allowPostponedAndSuspended) { await Task.Delay(250); continue; }
                    throw new InvocationSuspendedException(flowId);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}