using System;
using System.Text.Json;
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
            async Task(string param, Context context) =>
            {
                await context.Activity
                    .Do(
                        "id",
                        work: () =>
                        {
                            counter.Increment();
                            throw new PostponeInvocationException(1);
                        }, ResiliencyLevel.AtMostOnce
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
            async Task(string param, Context context) =>
            {
                await context.Activity
                    .Do(
                        "someId",
                        work: () =>
                        {
                            counter.Increment();
                            if (counter.Current != 0)
                                throw new PostponeInvocationException(1);

                            return "hello world".ToTask();
                        }, ResiliencyLevel.AtMostOnce
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
            async Task(string param, Context context) =>
            {
                await context.Activity
                    .Do(
                        "someId",
                        work: () =>
                        {
                            counter.Increment();
                            if (counter.Current != 0)
                                throw new PostponeInvocationException(1);

                            return new Person("Peter", 32).ToTask();
                        }, ResiliencyLevel.AtMostOnce
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
            async Task(string param, Context context) =>
            {
                await context.Activity
                    .Do(
                        "id",
                        work: () => { counter.Increment(); return 1.ToTask(); }, 
                        ResiliencyLevel.AtMostOnce
                    );
            });

        await rAction.Invoke(functionInstanceId.ToString(), "hello");
        await rAction.ControlPanel(functionInstanceId).Result!.ReInvoke();

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
            async Task(string param, Context context) =>
            {
                await context.Activity
                    .Do(
                        "someId",
                        work: () => { counter.Increment(); return "hello world".ToTask(); }, 
                        ResiliencyLevel.AtMostOnce
                    );
            });

        await rAction.Invoke(functionInstanceId.ToString(), "hello");
        var controlPanel = await rAction.ControlPanel(functionInstanceId.ToString());
        controlPanel.ShouldNotBeNull();

        await controlPanel.ReInvoke();

        counter.Current.ShouldBe(1);
        await controlPanel.Refresh();

        var value = await controlPanel.Activities.SelectAsync(a => a.GetValue<string>("someId"));
        value.ShouldBe("hello world");
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
            async Task(string param, Context context) =>
            {
                await context.Activity
                    .Do(
                        "someId",
                        work: () => { counter.Increment(); return new Person("Peter", 32).ToTask(); }, 
                        ResiliencyLevel.AtMostOnce
                    );
            });

        await rAction.Invoke(functionInstanceId.ToString(), "hello");
        var controlPanel = await rAction.ControlPanel(functionInstanceId.ToString());
        controlPanel.ShouldNotBeNull();

        await controlPanel.ReInvoke();

        counter.Current.ShouldBe(1);
        await controlPanel.Refresh();

        var value = await controlPanel.Activities.SelectAsync(a => a.GetValue<Person>("someId"));
        value.ShouldBe(new Person("Peter", 32));
    }
}