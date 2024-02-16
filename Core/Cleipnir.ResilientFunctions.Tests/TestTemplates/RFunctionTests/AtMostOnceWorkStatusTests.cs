using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
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
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(postponedCheckFrequency: TimeSpan.FromMilliseconds(100)));
        var counter = new SyncedCounter();
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;

        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            async Task(string param, Workflow workflow) =>
            {
                await workflow.Activities
                    .Do(
                        "Id",
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
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(postponedCheckFrequency: TimeSpan.FromMilliseconds(100)));
        var counter = new SyncedCounter();
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            async Task(string param, Workflow workflow) =>
            {
                await workflow.Activities
                    .Do(
                        "someId",
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
    
    public abstract Task CompletedAtMostOnceWorkIsNotExecutedMultipleTimes();
    public async Task CompletedAtMostOnceWorkIsNotExecutedMultipleTimes(Task<IFunctionStore> functionStoreTask)
    {
        var store = await functionStoreTask;
        using var functionsRegistry = new FunctionsRegistry(store);
        var counter = new SyncedCounter();
        var functionId = TestFunctionId.Create();
        var (functionTypeId, functionInstanceId) = functionId;
        
        var rAction = functionsRegistry.RegisterAction(
            functionTypeId,
            async Task(string param, Workflow workflow) =>
            {
                await workflow.Activities
                    .Do(
                        id: "id",
                        work: () =>
                        {
                            counter.Increment();
                            return Task.CompletedTask;
                        }, ResiliencyLevel.AtMostOnce
                    );
            });

        await rAction.Invoke(functionInstanceId.ToString(), "hello");
        await rAction.ControlPanel(functionInstanceId).Result!.ReInvoke();

        counter.Current.ShouldBe(1);
    }
}