using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Newtonsoft.Json;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;

public abstract class InitialInvocationFailedTests
{
    public abstract Task CreatedActionIsCompletedByWatchdog();
    protected async Task CreatedActionIsCompletedByWatchdog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        await store.CreateFunction(
            functionId,
            param: "hello world".ToJson(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            reference: Guid.NewGuid()
        );

        var flag = new SyncedFlag();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(leaseLength: TimeSpan.FromMilliseconds(100)));
        _ = functionsRegistry.RegisterAction(
            functionId.Type,
            Task (string param) =>
            {
                flag.Raise();
                return Task.CompletedTask;
            });

        await flag.WaitForRaised();

        await BusyWait.Until(
            () => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded)
        );
    }

    public abstract Task CreatedActionWithStateIsCompletedByWatchdog();
    protected async Task CreatedActionWithStateIsCompletedByWatchdog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        await store.CreateFunction(
            functionId,
            param: "hello world".ToJson(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            reference: Guid.NewGuid()
        );

        var flag = new SyncedFlag();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(leaseLength: TimeSpan.FromMilliseconds(100)));
        _ = functionsRegistry.RegisterAction<string>(
            functionId.Type,
            Task (string param) =>
            {
                flag.Raise();
                return Task.CompletedTask;
            });

        await flag.WaitForRaised();

        await BusyWait.Until(
            () => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded)
        );
    }

    public abstract Task CreatedFuncIsCompletedByWatchdog();
    public async Task CreatedFuncIsCompletedByWatchdog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        await store.CreateFunction(
            functionId,
            param: "hello world".ToJson(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            reference: Guid.NewGuid()
        );

        var flag = new SyncedFlag();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(leaseLength: TimeSpan.FromMilliseconds(100)));
        _ = functionsRegistry.RegisterFunc(
            functionId.Type,
            Task<string> (string param) =>
            {
                flag.Raise();
                return param.ToUpper().ToTask();
            });

        await flag.WaitForRaised();

        await BusyWait.Until(
            () => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded)
        );
        var resultJson = await store.GetFunction(functionId).Map(sf => sf?.Result);
        resultJson.ShouldNotBeNull();
        JsonConvert.DeserializeObject<string>(resultJson).ShouldBe("HELLO WORLD");
    }

    public abstract Task CreatedFuncWithStateIsCompletedByWatchdog();
    protected async Task CreatedFuncWithStateIsCompletedByWatchdog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFlowId.Create();
        await store.CreateFunction(
            functionId,
            param: "hello world".ToJson(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            reference: Guid.NewGuid()
        );

        var flag = new SyncedFlag();
        using var functionsRegistry = new FunctionsRegistry(store, new Settings(leaseLength: TimeSpan.FromMilliseconds(100)));
        _ = functionsRegistry.RegisterFunc(
            functionId.Type,
            Task<string> (string param) =>
            {
                flag.Raise();
                return param.ToUpper().ToTask();
            });

        await flag.WaitForRaised();

        await BusyWait.Until(
            () => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded)
        );
        
        var resultJson = await store.GetFunction(functionId).Map(sf => sf?.Result);
        resultJson.ShouldNotBeNull();
        JsonConvert.DeserializeObject<string>(resultJson).ShouldBe("HELLO WORLD");
    }
}