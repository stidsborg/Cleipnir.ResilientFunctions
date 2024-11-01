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

        var functionId = new FlowId("flowType", "flowInstance");
     
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        using var rFunctions = new FunctionsRegistry(store, new Settings(
            leaseLength: TimeSpan.FromMilliseconds(10),
            delayStartup: TimeSpan.FromSeconds(1))
        );
        var registration = rFunctions.RegisterAction(
            functionId.Type,
            Task (string param) => Task.CompletedTask
        );
        await store.CreateFunction(
            registration.MapToStoredId(functionId),
            "hello world".ToJson().ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );

        await BusyWait.Until(() => store.GetFunction(registration.MapToStoredId(functionId)).Map(sf => sf?.Status == Status.Succeeded));
        stopWatch.Elapsed.ShouldBeGreaterThan(TimeSpan.FromMilliseconds(750));
    }
    
    [TestMethod]
    public async Task CrashedWatchDogStartUpNotDelayedSuccessfully()
    {
        var store = new InMemoryFunctionStore();

        var functionId = new FlowId("flowType", "flowInstance");
       
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        using var rFunctions = new FunctionsRegistry(store, new Settings(leaseLength: TimeSpan.FromMilliseconds(10)));
        var registration = rFunctions.RegisterAction(
            functionId.Type,
            Task (string param) => Task.CompletedTask
        );
        await store.CreateFunction(
            registration.MapToStoredId(functionId),
            "hello world".ToJson().ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );

        await BusyWait.Until(() => store.GetFunction(registration.MapToStoredId(functionId)).Map(sf => sf?.Status == Status.Succeeded));
        stopWatch.Elapsed.ShouldBeLessThan(TimeSpan.FromMilliseconds(500));
    }
    
    [TestMethod]
    public async Task PostponedWatchDogStartUpIsDelayedByOneSecondSuccessfully()
    {
        var store = new InMemoryFunctionStore();
        var storedParameter = "hello world".ToJson();
        var functionId = new FlowId("flowType", "flowInstance"); 
      

        var stopWatch = new Stopwatch();
        stopWatch.Start();
        using var rFunctions = new FunctionsRegistry(store, new Settings(
            watchdogCheckFrequency: TimeSpan.FromMilliseconds(10),
            delayStartup: TimeSpan.FromSeconds(1))
        );
        var registration = rFunctions.RegisterAction(
            functionId.Type,
            Task (string param) => Task.CompletedTask
        );

        await store.CreateFunction(
            registration.MapToStoredId(functionId),
            storedParameter.ToUtf8Bytes(),
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        await store.PostponeFunction(
            registration.MapToStoredId(functionId),
            postponeUntil: 0,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            complimentaryState: new ComplimentaryState(storedParameter.ToUtf8Bytes().ToFunc(), LeaseLength: 0)
        ).ShouldBeTrueAsync();
        
        await BusyWait.Until(() => store.GetFunction(registration.MapToStoredId(functionId)).Map(sf => sf?.Status == Status.Succeeded));
        stopWatch.Elapsed.ShouldBeGreaterThan(TimeSpan.FromMilliseconds(750));
    }
    
    [TestMethod]
    public async Task PostponedWatchDogStartUpNotDelayedSuccessfully()
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
            registration.MapToStoredId(functionId),
            storedParameter,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        );
        await store.PostponeFunction(
            registration.MapToStoredId(functionId),
            postponeUntil: 0,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch: 0,
            new ComplimentaryState(storedParameter.ToFunc(), LeaseLength: 0)
        );

        await BusyWait.Until(() => store.GetFunction(registration.MapToStoredId(functionId)).Map(sf => sf?.Status == Status.Succeeded));
        stopWatch.Elapsed.ShouldBeLessThan(TimeSpan.FromMilliseconds(500));
    }
}