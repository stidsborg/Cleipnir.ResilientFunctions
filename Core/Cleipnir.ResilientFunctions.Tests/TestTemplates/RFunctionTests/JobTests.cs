using System;
using System.Linq;
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
        using var rFunctions = new FunctionContainer(store);
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
            .GetFunction(new FunctionId(nameof(JobCanBeRetried), "Job"))
            .Map(sf => sf?.Status == Status.Postponed)
        );

        await rJob.Retry(expectedStatuses: new [] { Status.Postponed });

        await BusyWait.UntilAsync(() => counter.Current == 2);
    }

    public abstract Task JobCanBeStartedMultipleTimesWithoutError();
    protected async Task JobCanBeStartedMultipleTimesWithoutError(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var rFunctions = new FunctionContainer(store);
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
    
    public abstract Task CrashedJobIsRetried();
    protected async Task CrashedJobIsRetried(Task<IFunctionStore> storeTask)
    {
        var functionId = new FunctionId(nameof(CrashedJobIsRetried), "Job");
        using var disposables = new CombinableDisposable();
        var store = await storeTask;
        {
            using var rFunctions = new FunctionContainer(store, new Settings(CrashedCheckFrequency: TimeSpan.FromDays(1)));
            disposables.Add(rFunctions);
            var rJob = rFunctions.RegisterJob<EmptyScrapbook>(
                nameof(CrashedJobIsRetried),
                inner: _ => NeverCompletingTask.OfVoidType
            );
            await rJob.Start();
        }
        {
            var flag = new SyncedFlag();
            using var rFunctions = new FunctionContainer(store, new Settings(CrashedCheckFrequency: TimeSpan.FromMilliseconds(10)));
            disposables.Add(rFunctions);
            rFunctions.RegisterJob<EmptyScrapbook>(
                nameof(CrashedJobIsRetried),
                inner: _ => flag.Raise()
            );
            
            await BusyWait.Until(
                () => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded)
            );
            
            flag.Position.ShouldBe(FlagPosition.Raised);
        }
    }
    
    public abstract Task PostponedJobDoesNotCauseUnhandledException();
    protected async Task PostponedJobDoesNotCauseUnhandledException(Task<IFunctionStore> storeTask)
    {
        using var disposables = new CombinableDisposable();
        var store = await storeTask;
        var unhandledExceptions = new UnhandledExceptionCatcher();
        using var rFunctions = new FunctionContainer(
            store,
            new Settings(
                unhandledExceptions.Catch,
                TimeSpan.FromDays(1)
            )
        );
        disposables.Add(rFunctions);
        var rJob = rFunctions.RegisterJob<EmptyScrapbook>(
            nameof(PostponedJobDoesNotCauseUnhandledException),
            inner: _ => Postpone.For(TimeSpan.FromMilliseconds(10))
        );
        await rJob.Start();

        await Task.Delay(100);

        unhandledExceptions.ThrownExceptions.ShouldBeEmpty();
    }
    
    public abstract Task FailedJobDoesCausesUnhandledException();
    protected async Task FailedJobDoesCausesUnhandledException(Task<IFunctionStore> storeTask)
    {
        using var disposables = new CombinableDisposable();
        var store = await storeTask;
        var unhandledExceptions = new UnhandledExceptionCatcher();
        using var rFunctions = new FunctionContainer(
            store,
            new Settings(unhandledExceptions.Catch, CrashedCheckFrequency: TimeSpan.FromDays(1))
        );
        disposables.Add(rFunctions);
        var rJob = rFunctions.RegisterJob<EmptyScrapbook>(
            nameof(FailedJobDoesCausesUnhandledException),
            inner: _ => Fail.WithException(new Exception("oh no"))
        );
        await rJob.Start();

        await BusyWait.UntilAsync(() => unhandledExceptions.ThrownExceptions.Any());
    }
}