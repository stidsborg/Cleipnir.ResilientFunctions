using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class AtMostOnceWorkStatusTests
{
    public abstract Task AtMostOnceWorkIsNotExecutedMultipleTimes();
    public async Task AtMostOnceWorkIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;

        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            async Task(string param, Scrapbook scrapbook) =>
            {
                await scrapbook
                    .DoAtMostOnce(
                        workStatus: s => s.WorkStatus,
                        work: () =>
                        {
                            counter.Increment();
                            throw new PostponeInvocationException(1);
                        }
                    );
            });

        await rAction.Schedule(functionInstanceId.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(functionId)
                .SelectAsync(sf => sf?.Status == Status.Failed)
        );
        
        counter.Current.ShouldBe(1);
    }
    
    public abstract Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes();
    public async Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            async Task(string param, Scrapbook scrapbook) =>
            {
                await scrapbook
                    .DoAtMostOnce(
                        workId: "someId",
                        work: () =>
                        {
                            counter.Increment();
                            throw new PostponeInvocationException(1);
                        }
                    );
            });

        await rAction.Schedule(functionInstanceId.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(functionId)
                .SelectAsync(sf => sf?.Status == Status.Failed)
        );
        
        counter.Current.ShouldBe(1);
    }
    
    public abstract Task CompletedAtMostOnceWorkIsNotExecutedMultipleTimes();
    public async Task CompletedAtMostOnceWorkIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            async Task(string param, Scrapbook scrapbook) =>
            {
                await scrapbook
                    .DoAtMostOnce(
                        workStatus: s => s.WorkStatus,
                        work: () => { counter.Increment(); return Task.CompletedTask; });
            });

        await rAction.Invoke(functionInstanceId.ToString(), "hello");
        await rAction.ControlPanel(functionInstanceId).Result!.ReInvoke();

        counter.Current.ShouldBe(1);
    }
    
    public abstract Task ReferencingGetOnlyPropertyThrowsException();
    public async Task ReferencingGetOnlyPropertyThrowsException(Task<IFunctionStore> functionStoreTask)
    {
        var scrapbook = new ScrapbookGetterOnly();
        await Should.ThrowAsync<ArgumentException>(() => 
            scrapbook.DoAtMostOnce(
                workStatus: s => s.WorkStatus,
                work: () => Task.CompletedTask
            )
        );
    }

    private class Scrapbook : RScrapbook
    {
        public WorkStatus WorkStatus { get; set; }
    }
    
    private class ScrapbookGetterOnly : RScrapbook
    {
        public WorkStatus WorkStatus { get; } = WorkStatus.NotStarted;
    }
}