using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.FunctionTests;

public abstract class AtLeastOnceWorkStatusAndResultTests
{
    public abstract Task AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted();
    public async Task AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)));
        var counter = new SyncedCounter();
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                await workflow.Effect
                    .Capture(async () =>
                        {
                            counter.Increment();
                            await workflow.Delay(TimeSpan.FromMilliseconds(100));
                            return 1;
                        }
                    );
            });

        await rAction.Schedule(flowInstance.ToString(), "hello");
        
        var controlPanel = await rAction.ControlPanel(flowInstance);
        controlPanel.ShouldNotBeNull();
        await controlPanel.WaitForCompletion(allowPostponeAndSuspended: true);
        
        await controlPanel.Effects.GetValue<int>(0).ShouldBeAsync(1);
        counter.Current.ShouldBe(2);
    }

    public abstract Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted();
    public async Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)));
        var counter = new SyncedCounter();
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            async Task<string>(string param, Workflow workflow) =>
            {
                return await workflow.Effect
                    .Capture(async () =>
                        {
                            counter.Increment();
                            if (counter.Current == 1)
                            {
                                await workflow.Delay(TimeSpan.FromMilliseconds(10));
                                return "nothing";
                            }

                            return "hello world";
                        }
                    );
            });

        _ = rFunc.Schedule(flowInstance.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(rFunc.MapToStoredId(functionId.Instance))
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
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)));
        var counter = new SyncedCounter();
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var rFunc = functionsRegistry.RegisterFunc(
            flowType,
            async Task<Person>(string param, Workflow workflow) =>
            {
                return await workflow.Effect
                    .Capture(async () =>
                        {
                            counter.Increment();
                            if (counter.Current == 1)
                            {
                                await workflow.Delay(TimeSpan.FromMilliseconds(10));
                                return null!;
                            }

                            return new Person("Peter", 32);
                        }
                    );
            });

        _ = rFunc.Schedule(flowInstance.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(rFunc.MapToStoredId(functionId.Instance))
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
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        using var functionsRegistry = new FunctionsRegistry(store);
        var counter = new SyncedCounter();
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                await workflow.Effect
                    .Capture(() => { counter.Increment(); return 1.ToTask(); }
                    );
            });

        await rAction.Invoke(flowInstance.ToString(), "hello");
        await BusyWait.Until(async () => await store.GetFunction(rAction.MapToStoredId(functionId.Instance)) != null);
        await rAction.ControlPanel(flowInstance).Result!.Restart();

        counter.Current.ShouldBe(1);
    }

    public abstract Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes();
    public async Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var counter = new SyncedCounter();
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                await workflow.Effect
                    .Capture(() => { counter.Increment(); return "hello world".ToTask(); }
                    );
            });

        await rAction.Invoke(flowInstance.ToString(), "hello");
        var controlPanel = await rAction.ControlPanel(flowInstance);
        controlPanel.ShouldNotBeNull();
        
        await controlPanel.Restart();
        await controlPanel.Refresh();

        var value = controlPanel.Effects.GetValue<string>(0);
        await value.ShouldBeAsync("hello world");
        counter.Current.ShouldBe(1);
    }
}