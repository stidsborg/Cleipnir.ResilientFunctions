using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class JobTests
{
    public abstract Task JobCanBeRetried();
    protected async Task JobCanBeRetried(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        var rJob = rFunctions.RegisterJob<EmptyScrapbook>(
            nameof(JobCanBeRetried),
            _ =>
            {
                counter.Increment();
                return Postpone.For(TimeSpan.FromDays(1));
            });
        
        await rJob.Start();
        await BusyWait.Until(() => store
            .GetFunction(new FunctionId("Job", nameof(JobCanBeRetried)))
            .Map(sf => sf?.Status == Status.Postponed)
        );

        await rJob.Retry(expectedStatuses: new [] { Status.Postponed });

        await BusyWait.UntilAsync(() => counter.Current == 2);
    }

    public abstract Task JobCanBeStartedMultipleTimesWithoutError();
    protected async Task JobCanBeStartedMultipleTimesWithoutError(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        var rJob = rFunctions.RegisterJob<EmptyScrapbook>(
            nameof(JobCanBeRetried),
            _ =>
            {
                counter.Increment();
                return Postpone.For(TimeSpan.FromDays(1));
            });
        
        var t1 = rJob.Start();
        var t2 = rJob.Start();

        await Task.Delay(500);
        counter.Current.ShouldBe(1);
        await t1; //should not throw exception
        await t2; //should not throw exception
    }
}