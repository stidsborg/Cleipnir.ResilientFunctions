using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class WatchdogStartUpTests
{
    [TestMethod]
    public async Task CrashedFlowIsPickedUpWithoutDelay()
    {
        var store = new InMemoryFunctionStore();

        var flowId = TestFlowId.Create();

        var storedType = await store.TypeStore.InsertOrGetStoredType(flowId.Type);
        var storedId = StoredId.Create(storedType, flowId.Instance.Value);
        await store.CreateFunction(
            storedId,
            "humanInstanceId",
            "hello world".ToJson().ToUtf8Bytes(),
            postponeUntil: 0,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: null
        );

        var stopWatch = new Stopwatch();
        stopWatch.Start();

        using var rFunctions = new FunctionsRegistry(store);
        rFunctions.RegisterAction(
            flowId.Type,
            Task (string param) => Task.CompletedTask
        );

        await BusyWait.Until(() => store.GetFunction(storedId).Map(sf => sf?.Status == Status.Succeeded));
        stopWatch.Elapsed.ShouldBeLessThan(TimeSpan.FromMilliseconds(500));
    }

    [TestMethod]
    public async Task PostponedFlowIsPickedUpWithoutDelay()
    {
        var store = new InMemoryFunctionStore();

        var storedParameter = "hello world".ToJson().ToUtf8Bytes();
        var functionId = new FlowId("flowType", "flowInstance");

        var stopWatch = new Stopwatch();
        stopWatch.Start();
        using var rFunctions = new FunctionsRegistry(store, new Settings(watchdogCheckFrequency: TimeSpan.FromMilliseconds(10)));
        var registration = rFunctions.RegisterAction(
            functionId.Type,
            Task (string param) => Task.CompletedTask
        );

        await store.CreateFunction(
            registration.MapToStoredId(functionId.Instance),
            "humanInstanceId",
            storedParameter,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks,
            parent: null,
            owner: ReplicaId.Empty
        );
        await store.SetStatus(
            registration.MapToStoredId(functionId.Instance),
            Status.Postponed,
            result: null,
            storedException: null,
            expires: 0,
            timestamp: DateTime.UtcNow.Ticks,
            expectedReplica: ReplicaId.Empty,
            storageSession: null
        );

        await BusyWait.Until(() => store.GetFunction(registration.MapToStoredId(functionId.Instance)).Map(sf => sf?.Status == Status.Succeeded));
        stopWatch.Elapsed.ShouldBeLessThan(TimeSpan.FromMilliseconds(500));
    }
}
