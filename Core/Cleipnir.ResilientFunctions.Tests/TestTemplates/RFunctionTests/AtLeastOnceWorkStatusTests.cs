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

public abstract class AtLeastOnceWorkStatusTests
{
    public abstract Task AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted();
    public async Task AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted(Task<IFunctionStore> functionStoreTask)
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
                    .DoAtLeastOnce(
                        workStatus: s => s.WorkStatus,
                        work: () =>
                        {
                            counter.Increment();
                            if (counter.Current == 1)
                                throw new PostponeInvocationException(1);
                            return Task.CompletedTask;
                        }
                    );
            });

        await rAction.Schedule(functionInstanceId.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(functionId)
                .SelectAsync(sf => sf?.Status == Status.Succeeded)
        );

        counter.Current.ShouldBe(2);
    }
    
    public abstract Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted();
    public async Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted(Task<IFunctionStore> functionStoreTask)
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
                    .DoAtLeastOnce(
                        workId: "someId",
                        work: () =>
                        {
                            counter.Increment();
                            if (counter.Current == 1)
                                throw new PostponeInvocationException(1);
                            return Task.CompletedTask;
                        }
                    );
            });

        await rAction.Schedule(functionInstanceId.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(functionId)
                .SelectAsync(sf => sf?.Status == Status.Succeeded)
        );

        counter.Current.ShouldBe(2);
    }
    
    public abstract Task CompletedAtLeastOnceWorkIsNotExecutedMultipleTimes();
    public async Task CompletedAtLeastOnceWorkIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
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
                    .DoAtLeastOnce(
                        workStatus: s => s.WorkStatus,
                        work: () => { counter.Increment(); return Task.CompletedTask; });
            });

        await rAction.Invoke(functionInstanceId.ToString(), "hello");
        await rAction.ControlPanel(functionInstanceId).Result!.ReInvoke();

        counter.Current.ShouldBe(1);
    }
    
    public abstract Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes();
    public async Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
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
                    .DoAtLeastOnce(
                        workId: "someId",
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