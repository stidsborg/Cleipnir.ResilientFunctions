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
public class DelayedStartUpTests
{
    [TestMethod]
    public async Task CrashedWatchDogStartUpIsDelayedByOneSecondSuccessfully()
    {
        var store = new InMemoryFunctionStore();

        var functionId = new FunctionId("FunctionTypeId", "FunctionInstanceId");
        await store.CreateFunction(
            functionId,
            "hello world".ToJson(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        using var rFunctions = new FunctionsRegistry(store, new Settings(
            leaseLength: TimeSpan.FromMilliseconds(10),
            delayStartup: TimeSpan.FromSeconds(1))
        );
        rFunctions.RegisterAction(
            functionId.TypeId,
            void(string param) => { }
        );

        await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded));
        stopWatch.Elapsed.ShouldBeGreaterThan(TimeSpan.FromMilliseconds(750));
    }
    
    [TestMethod]
    public async Task CrashedWatchDogStartUpNotDelayedSuccessfully()
    {
        var store = new InMemoryFunctionStore();

        var functionId = new FunctionId("FunctionTypeId", "FunctionInstanceId");
        await store.CreateFunction(
            functionId,
            "hello world".ToJson(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        using var rFunctions = new FunctionsRegistry(store, new Settings(leaseLength: TimeSpan.FromMilliseconds(10)));
        rFunctions.RegisterAction(
            functionId.TypeId,
            void(string param) => { }
        );

        await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded));
        stopWatch.Elapsed.ShouldBeLessThan(TimeSpan.FromMilliseconds(500));
    }
    
    [TestMethod]
    public async Task PostponedWatchDogStartUpIsDelayedByOneSecondSuccessfully()
    {
        var store = new InMemoryFunctionStore();
        var storedParameter = "hello world".ToJson();
        var functionId = new FunctionId("FunctionTypeId", "FunctionInstanceId"); 
        await store.CreateFunction(
            functionId,
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        await store.PostponeFunction(
            functionId,
            postponeUntil: 0,
            defaultState: null,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(storedParameter.ToFunc(), LeaseLength: 0)
        ).ShouldBeTrueAsync();

        var stopWatch = new Stopwatch();
        stopWatch.Start();
        using var rFunctions = new FunctionsRegistry(store, new Settings(
            watchdogCheckFrequency: TimeSpan.FromMilliseconds(10),
            delayStartup: TimeSpan.FromSeconds(1))
        );
        rFunctions.RegisterAction(
            functionId.TypeId,
            void(string param) => { }
        );

        await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded));
        stopWatch.Elapsed.ShouldBeGreaterThan(TimeSpan.FromMilliseconds(750));
    }
    
    [TestMethod]
    public async Task PostponedWatchDogStartUpNotDelayedSuccessfully()
    {
        var store = new InMemoryFunctionStore();

        var storedParameter = "hello world".ToJson();
        var functionId = new FunctionId("FunctionTypeId", "FunctionInstanceId");
        await store.CreateFunction(
            functionId,
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        await store.PostponeFunction(
            functionId,
            postponeUntil: 0,
            defaultState: null,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            new ComplimentaryState(storedParameter.ToFunc(), LeaseLength: 0)
        );

        var stopWatch = new Stopwatch();
        stopWatch.Start();
        using var rFunctions = new FunctionsRegistry(store, new Settings(watchdogCheckFrequency: TimeSpan.FromMilliseconds(10)));
        rFunctions.RegisterAction(
            functionId.TypeId,
            void(string param) => { }
        );

        await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded));
        stopWatch.Elapsed.ShouldBeLessThan(TimeSpan.FromMilliseconds(500));
    }
}