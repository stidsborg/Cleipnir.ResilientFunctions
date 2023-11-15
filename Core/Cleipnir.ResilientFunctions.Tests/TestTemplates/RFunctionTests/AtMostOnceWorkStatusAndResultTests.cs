using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Reactive.Extensions.Work;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class AtMostOnceWorkStatusAndResultTests
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
                            if (counter.Current != 0)
                                throw new PostponeInvocationException(1);

                            return "hello world".ToTask();
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
    
    public abstract Task AtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes();
    public async Task AtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
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
                            if (counter.Current != 0)
                                throw new PostponeInvocationException(1);

                            return new Person("Peter", 32).ToTask();
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

    private record Person(string Name, int Age);
    
    public abstract Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource();
    public async Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            async Task(string param, Context context) =>
            {
                var es = context.EventSource;
                await es
                    .DoAtMostOnce(
                        workId: "someId",
                        work: () =>
                        {
                            counter.Increment();
                            if (counter.Current != 0)
                                throw new PostponeInvocationException(1);

                            return "hello world".ToTask();
                        }
                    );
            });

        await rAction.Schedule(functionInstanceId.ToString(), "hello");

        var controlPanel = await rAction.ControlPanels.For(functionInstanceId);
        controlPanel.ShouldNotBeNull();
        
        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Failed;
        });

        var events = await controlPanel.Events;
        events.ExistingCount.ShouldBe(1);
        events.OfType<WorkStarted>().Single().WorkId.ShouldBe("someId");
        
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
                        work: () => { counter.Increment(); return 1.ToTask(); }
                    );
            });

        await rAction.Invoke(functionInstanceId.ToString(), "hello");
        await rAction.ControlPanels.For(functionInstanceId).Result!.ReInvoke();

        counter.Current.ShouldBe(1);
    }
    
    public abstract Task CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes();
    public async Task CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
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
                        work: () => { counter.Increment(); return "hello world".ToTask(); }
                    );
            });

        await rAction.Invoke(functionInstanceId.ToString(), "hello");
        var controlPanel = await rAction.ControlPanels.For(functionInstanceId.ToString());
        controlPanel.ShouldNotBeNull();

        await controlPanel.ReInvoke();

        counter.Current.ShouldBe(1);
        await controlPanel.Refresh();

        var value = controlPanel.Scrapbook.StateDictionary["someId"];
        var splitValue = value.Split(",");
        splitValue[0].ShouldBe(WorkStatus.Completed.ToString());
        splitValue[1].ShouldBe("hello world");
    }
    
    public abstract Task CompletedAtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes();
    public async Task CompletedAtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
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
                        work: () => { counter.Increment(); return new Person("Peter", 32).ToTask(); }
                    );
            });

        await rAction.Invoke(functionInstanceId.ToString(), "hello");
        var controlPanel = await rAction.ControlPanels.For(functionInstanceId.ToString());
        controlPanel.ShouldNotBeNull();

        await controlPanel.ReInvoke();

        counter.Current.ShouldBe(1);
        await controlPanel.Refresh();

        var value = controlPanel.Scrapbook.StateDictionary["someId"];
        var deserialized = JsonSerializer.Deserialize<Work<Person>>(value);
        deserialized.Status.ShouldBe(WorkStatus.Completed);
        deserialized.Result.ShouldBe(new Person("Peter", 32));
    }
    
    public abstract Task CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource();
    public async Task CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimesUsingEventSource(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        using var rFunctions = new RFunctions(store);
        var counter = new SyncedCounter();
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        var rAction = rFunctions.RegisterAction(
            functionTypeId,
            async Task(string param, Context context) =>
            {
                var es = context.EventSource;
                await es
                    .DoAtMostOnce(
                        workId: "someId",
                        work: () => { counter.Increment(); return "hello world".ToTask(); }
                    );
            });

        await rAction.Schedule(functionInstanceId.ToString(), "hello");
        var controlPanel = await rAction.ControlPanels.For(functionInstanceId);
        controlPanel.ShouldNotBeNull();

        await BusyWait.Until(async () =>
        {
            await controlPanel.Refresh();
            return controlPanel.Status == Status.Succeeded;
        });

        await controlPanel.ReInvoke();

        counter.Current.ShouldBe(1);
        await controlPanel.Refresh();

        var events = await controlPanel.Events;
        events.ExistingCount.ShouldBe(2);
        events.OfType<WorkStarted>().Single().WorkId.ShouldBe("someId");
        var completedWork = events.OfType<WorkWithResultCompleted<string>>().Single();
        completedWork.WorkId.ShouldBe("someId");
        completedWork.Result.ShouldBe("hello world");
    }
   
    public abstract Task ReferencingGetOnlyPropertyThrowsException();
    public async Task ReferencingGetOnlyPropertyThrowsException(Task<IFunctionStore> functionStoreTask)
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
        public Work<int> WorkStatus { get; set; }
    }
    
    private class ScrapbookGetterOnly : RScrapbook
    {
        public Work<int> WorkStatus { get; } = new();
    }
}