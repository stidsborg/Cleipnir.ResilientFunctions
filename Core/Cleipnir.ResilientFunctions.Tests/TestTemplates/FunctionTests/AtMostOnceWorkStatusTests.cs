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

public abstract class AtMostOnceWorkStatusTests
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
                        "Id",
                        work: async () =>
                        {
                            counter.Increment();
                            await workflow.Delay(TimeSpan.FromMilliseconds(100));
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
                        work: async () =>
                        {
                            counter.Increment();
                            await workflow.Delay(TimeSpan.FromMilliseconds(10));
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
    
    public abstract Task CompletedAtMostOnceWorkIsNotExecutedMultipleTimes();
    public async Task CompletedAtMostOnceWorkIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
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
                        "id",
                        work: () =>
                        {
                            counter.Increment();
                            return Task.CompletedTask;
                        }, ResiliencyLevel.AtMostOnce
                    );
            });

        await rAction.Invoke(flowInstance.ToString(), "hello");
        await rAction.ControlPanel(flowInstance).Result!.Restart();

        counter.Current.ShouldBe(1);
    }
}