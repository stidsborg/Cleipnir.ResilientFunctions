using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class AtLeastOnceWorkStatusAndResultTests
{
    public abstract Task AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted();
    public async Task AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
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
                            
                            return 1.ToTask();
                        }
                    );
            });

        _ = rAction.Invoke(functionInstanceId.ToString(), "hello");
        
        await BusyWait.Until(() =>
            store.GetFunction(functionId)
                .SelectAsync(sf => sf?.Status == Status.Succeeded)
        );

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId);
        controlPanel!.Scrapbook.WorkStatus.Result.ShouldBe(1);
        counter.Current.ShouldBe(2);
    }

    public abstract Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted();
    public async Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            async Task<string>(string param, Scrapbook scrapbook) =>
            {
                return await scrapbook
                    .DoAtLeastOnce(
                        workId: "someId",
                        work: () =>
                        {
                            counter.Increment();
                            if (counter.Current == 1)
                                throw new PostponeInvocationException(1);
                            
                            return "hello world".ToTask();
                        }
                    );
            });

        _ = rFunc.Schedule(functionInstanceId.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(functionId)
                .SelectAsync(sf => sf?.Status == Status.Succeeded)
        );

        counter.Current.ShouldBe(2);

        var result = await rFunc.Invoke(functionId.ToString(), "hello");
        result.ShouldBe("hello world");
    }
    
    public abstract Task AtLeastOnceWorkWithCallIdAndGenericResultIsExecutedMultipleTimesWhenNotCompleted();
    public async Task AtLeastOnceWorkWithCallIdAndGenericResultIsExecutedMultipleTimesWhenNotCompleted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            async Task<Person>(string param, Scrapbook scrapbook) =>
            {
                return await scrapbook
                    .DoAtLeastOnce(
                        workId: "someId",
                        work: () =>
                        {
                            counter.Increment();
                            if (counter.Current == 1)
                                throw new PostponeInvocationException(1);
                            
                            return new Person("Peter", 32).ToTask();
                        }
                    );
            });

        _ = rFunc.Schedule(functionInstanceId.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(functionId)
                .SelectAsync(sf => sf?.Status == Status.Succeeded)
        );

        counter.Current.ShouldBe(2);

        var result = await rFunc.Invoke(functionId.ToString(), "hello");
        result.ShouldBe(new Person("Peter", 32));
    }

    private record Person(string Name, int Age);
    
    public abstract Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompletedUsingEventSource();
    public async Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompletedUsingEventSource(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();

        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        var rFunc = rFunctions.RegisterFunc(
            functionTypeId,
            async Task<string>(string param, Context context) =>
            {
                var es = await context.EventSource;
                return await es
                    .DoAtLeastOnce(
                        workId: "someId",
                        work: () =>
                        {
                            counter.Increment();
                            if (counter.Current == 1)
                                throw new PostponeInvocationException(1);
                            
                            return "hello world".ToTask();
                        }
                    );
            });

        _ = rFunc.Schedule(functionInstanceId.ToString(), "hello");

        await BusyWait.Until(async () => await store.GetFunction(functionId) != null);

        var controlPanel = await rFunc.ControlPanels.For(functionInstanceId.ToString());
        controlPanel.ShouldNotBeNull();

        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });

        counter.Current.ShouldBe(2);
        var events = await controlPanel.Events;
        events.ExistingCount.ShouldBe(1);
        var workCompleted = events.OfType<WorkWithResultCompleted<string>>().Single();
        workCompleted.WorkId.ShouldBe("someId");
        
        controlPanel.Result.ShouldBe("hello world");
    }

    public abstract Task CompletedAtLeastOnceWorkIsNotExecutedMultipleTimes();
    public async Task CompletedAtLeastOnceWorkIsNotExecutedMultipleTimes(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            async Task(string param, Scrapbook scrapbook) =>
            {
                await scrapbook
                    .DoAtLeastOnce(
                        workStatus: s => s.WorkStatus,
                        work: () => { counter.Increment(); return 1.ToTask(); });
            });

        await rAction.Invoke(functionInstanceId.ToString(), "hello");
        await BusyWait.Until(async () => await store.GetFunction(functionId) != null);
        await rAction.ControlPanels.For(functionInstanceId).Result!.ReInvoke();

        counter.Current.ShouldBe(1);
    }

    public abstract Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes();
    public async Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
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
                        work: () => { counter.Increment(); return "hello world".ToTask(); }
                    );
            });

        await rAction.Invoke(functionInstanceId.ToString(), "hello");
        var controlPanel = await rAction.ControlPanels.For(functionInstanceId);
        controlPanel.ShouldNotBeNull();
        
        await controlPanel.ReInvoke();
        await controlPanel.Refresh();

        var value = controlPanel.Scrapbook.StateDictionary["someId"];
        var splitValue = value.Split(",");
        splitValue[0].ShouldBe(WorkStatus.Completed.ToString());
        splitValue[1].ShouldBe("hello world");
        counter.Current.ShouldBe(1);
    }
    
    public abstract Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource();
    public async Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            async Task(string param, Context context) =>
            {
                var es = await context.EventSource;
                await es
                    .DoAtLeastOnce(
                        workId: "someId",
                        work: () => { counter.Increment(); return "hello world".ToTask(); }
                    );
            });

        await rAction.Invoke(functionInstanceId.ToString(), "hello");
        var controlPanel = await rAction.ControlPanels.For(functionInstanceId);
        controlPanel.ShouldNotBeNull();
        
        await controlPanel.ReInvoke();
        await controlPanel.Refresh();

        var events = await controlPanel.Events;
        events.ExistingCount.ShouldBe(1);
        var workResult = events.OfType<WorkWithResultCompleted<string>>().Single();
        workResult.WorkId.ShouldBe("someId");
        workResult.Result.ShouldBe("hello world");
        counter.Current.ShouldBe(1);
    }

    public abstract Task ReferencingGetOnlyPropertyThrowsException();
    public async Task ReferencingGetOnlyPropertyThrowsException(Task<IFunctionStore> storeTask)
    {
        var scrapbook = new ScrapbookGetterOnly();
        await Should.ThrowAsync<ArgumentException>(() => 
            scrapbook.DoAtMostOnce(
                workStatus: s => s.WorkStatus,
                work: () => 1.ToTask()
            )
        );
    }

    private class Scrapbook : RScrapbook
    {
        public WorkStatusAndResult<int> WorkStatus { get; set; }
    }
    
    private class ScrapbookGetterOnly : RScrapbook
    {
        public WorkStatusAndResult<int> WorkStatus { get; } = new();
    }
}