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
            async Task(string param, Context context) =>
            {
                await context.Activity
                    .Do(
                        "id",
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

        var controlPanel = await rAction.ControlPanel(functionInstanceId);
        controlPanel!.Activities.GetValue<int>("id").ShouldBe(1);
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
            async Task<string>(string param, Context context) =>
            {
                return await context.Activity
                    .Do(
                        "someId",
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
            async Task<Person>(string param, Context context) =>
            {
                return await context.Activity
                    .Do(
                        "someId",
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
            async Task(string param, Context context) =>
            {
                await context.Activity
                    .Do(
                        "id",
                        work: () => { counter.Increment(); return 1.ToTask(); }
                    );
            });

        await rAction.Invoke(functionInstanceId.ToString(), "hello");
        await BusyWait.Until(async () => await store.GetFunction(functionId) != null);
        await rAction.ControlPanel(functionInstanceId).Result!.ReInvoke();

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
            async Task(string param, Context context) =>
            {
                await context.Activity
                    .Do(
                        "someId",
                        work: () => { counter.Increment(); return "hello world".ToTask(); }
                    );
            });

        await rAction.Invoke(functionInstanceId.ToString(), "hello");
        var controlPanel = await rAction.ControlPanel(functionInstanceId);
        controlPanel.ShouldNotBeNull();
        
        await controlPanel.ReInvoke();
        await controlPanel.Refresh();

        var value = controlPanel.Activities.GetValue<string>("someId");
        value.ShouldBe("hello world");
        counter.Current.ShouldBe(1);
    }
}