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

public abstract class AtMostOnceWorkStatusAndResultTests
{
    public abstract Task AtMostOnceWorkIsNotExecutedMultipleTimes();
    public async Task AtMostOnceWorkIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)));
        var counter = new SyncedCounter();
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                await workflow.Effect
                    .Capture(
                        "id",
                        work: () =>
                        {
                            counter.Increment();
                            throw new PostponeInvocationException(1);
                        }, ResiliencyLevel.AtMostOnce
                    );
            });

        await rAction.Schedule(flowInstance.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(rAction.MapToStoredId(functionId.Instance))
                .SelectAsync(sf => sf?.Status == Status.Failed)
        );
        
        counter.Current.ShouldBe(1);
    }
    
    public abstract Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes();
    public async Task AtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)));
        var counter = new SyncedCounter();
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                await workflow.Effect
                    .Capture(
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

        await rAction.Schedule(flowInstance.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(rAction.MapToStoredId(functionId.Instance))
                .SelectAsync(sf => sf?.Status == Status.Failed)
        );
        
        counter.Current.ShouldBe(1);
    }
    
    public abstract Task AtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes();
    public async Task AtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)));
        var counter = new SyncedCounter();
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                await workflow.Effect
                    .Capture(
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

        await rAction.Schedule(flowInstance.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(rAction.MapToStoredId(functionId.Instance))
                .SelectAsync(sf => sf?.Status == Status.Failed)
        );
        
        counter.Current.ShouldBe(1);
    }

    private record Person(string Name, int Age);
    
    public abstract Task CompletedAtMostOnceWorkIsNotExecutedMultipleTimes();
    public async Task CompletedAtMostOnceWorkIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)));
        var counter = new SyncedCounter();
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                await workflow.Effect
                    .Capture(
                        "id",
                        work: () => { counter.Increment(); return 1.ToTask(); }, 
                        ResiliencyLevel.AtMostOnce
                    );
            });

        await rAction.Invoke(flowInstance.ToString(), "hello");
        await rAction.ControlPanel(flowInstance).Result!.Restart();

        counter.Current.ShouldBe(1);
    }
    
    public abstract Task CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes();
    public async Task CompletedAtMostOnceWorkWithCallIdIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)));
        var counter = new SyncedCounter();
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                await workflow.Effect
                    .Capture(
                        "someId",
                        work: () => { counter.Increment(); return "hello world".ToTask(); }, 
                        ResiliencyLevel.AtMostOnce
                    );
            });

        await rAction.Invoke(flowInstance.ToString(), "hello");
        var controlPanel = await rAction.ControlPanel(flowInstance.ToString());
        controlPanel.ShouldNotBeNull();

        await controlPanel.Restart();

        counter.Current.ShouldBe(1);
        await controlPanel.Refresh();

        var value = controlPanel.Effects.GetValue<string>("someId");
        await value.ShouldBeAsync("hello world");
    }
    
    public abstract Task CompletedAtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes();
    public async Task CompletedAtMostOnceWorkWithCallIdAndGenericResultIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(watchdogCheckFrequency: TimeSpan.FromMilliseconds(100)));
        var counter = new SyncedCounter();
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                await workflow.Effect
                    .Capture(
                        "someId",
                        work: () => { counter.Increment(); return new Person("Peter", 32).ToTask(); }, 
                        ResiliencyLevel.AtMostOnce
                    );
            });

        await rAction.Invoke(flowInstance.ToString(), "hello");
        var controlPanel = await rAction.ControlPanel(flowInstance.ToString());
        controlPanel.ShouldNotBeNull();

        await controlPanel.Restart();

        counter.Current.ShouldBe(1);
        await controlPanel.Refresh();

        var value = controlPanel.Effects.GetValue<Person>("someId");
        await value.ShouldBeAsync(new Person("Peter", 32));
    }
}