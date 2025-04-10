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

public abstract class AtLeastOnceWorkStatusTests
{
    public abstract Task AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted();
    public async Task AtLeastOnceWorkIsExecutedMultipleTimesWhenNotCompleted(Task<IFunctionStore> functionStoreTask)
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
                        "Id",
                        work: () =>
                        {
                            counter.Increment();
                            if (counter.Current == 1)
                                throw new PostponeInvocationException(DateTime.UtcNow);
                            return Task.CompletedTask;
                        }
                    );
            });

        await rAction.Schedule(flowInstance.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(rAction.MapToStoredId(functionId.Instance))
                .SelectAsync(sf => sf?.Status == Status.Succeeded)
        );

        counter.Current.ShouldBe(2);
    }
    
    public abstract Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted();
    public async Task AtLeastOnceWorkWithCallIdIsExecutedMultipleTimesWhenNotCompleted(Task<IFunctionStore> functionStoreTask)
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
                            if (counter.Current == 1)
                                throw new PostponeInvocationException(DateTime.UtcNow);
                            return Task.CompletedTask;
                        }
                    );
            });

        await rAction.Schedule(flowInstance.ToString(), "hello");

        await BusyWait.Until(() =>
            store.GetFunction(rAction.MapToStoredId(functionId.Instance))
                .SelectAsync(sf => sf?.Status == Status.Succeeded)
        );

        counter.Current.ShouldBe(2);
    }
    
    public abstract Task CompletedAtLeastOnceWorkIsNotExecutedMultipleTimes();
    public async Task CompletedAtLeastOnceWorkIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var counter = new SyncedCounter();
        var functionId = TestFlowId.Create();
        var (flowType, flowInstance) = functionId;
        
        var rAction = functionsRegistry.RegisterAction(
            flowType,
            async Task(string param, Workflow workflow) =>
            {
                await workflow.Effect
                    .Capture(
                        "Id",
                        work: () => { counter.Increment(); return Task.CompletedTask; });
            });

        await rAction.Invoke(flowInstance.ToString(), "hello");
        await rAction.ControlPanel(flowInstance).Result!.Restart();

        counter.Current.ShouldBe(1);
    }
    
    public abstract Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes();
    public async Task CompletedAtLeastOnceWorkWithCallIdIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        using var functionsRegistry = new FunctionsRegistry(store);
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
                        work: () => { counter.Increment(); return Task.CompletedTask; });
            });

        await rAction.Invoke(flowInstance.ToString(), "hello");
        await rAction.ControlPanel(flowInstance).Result!.Restart();

        counter.Current.ShouldBe(1);
    }
}