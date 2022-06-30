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
            new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            scrapbookType: null,
            initialStatus: Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0
        );
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        using var rFunctions = new FunctionContainer(store, new Settings(
            CrashedCheckFrequency: TimeSpan.FromMilliseconds(10),
            DelayStartup: TimeSpan.FromSeconds(1))
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
            new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            scrapbookType: null,
            initialStatus: Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0
        );
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        using var rFunctions = new FunctionContainer(store, new Settings(CrashedCheckFrequency: TimeSpan.FromMilliseconds(10)));
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

        var functionId = new FunctionId("FunctionTypeId", "FunctionInstanceId");
        await store.CreateFunction(
            functionId,
            new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            scrapbookType: null,
            initialStatus: Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0
        );
        await store.SetFunctionState(
            functionId,
            status: Status.Postponed,
            scrapbookJson: null, result: null, errorJson: null,
            postponedUntil: 0, expectedEpoch: 0
        );
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        using var rFunctions = new FunctionContainer(store, new Settings(
            PostponedCheckFrequency: TimeSpan.FromMilliseconds(10),
            DelayStartup: TimeSpan.FromSeconds(1))
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

        var functionId = new FunctionId("FunctionTypeId", "FunctionInstanceId");
        await store.CreateFunction(
            functionId,
            new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            scrapbookType: null,
            initialStatus: Status.Executing,
            initialEpoch: 0,
            initialSignOfLife: 0
        );
        await store.SetFunctionState(
            functionId,
            status: Status.Postponed,
            scrapbookJson: null, result: null, errorJson: null,
            postponedUntil: 0, expectedEpoch: 0
        );
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        using var rFunctions = new FunctionContainer(store, new Settings(PostponedCheckFrequency: TimeSpan.FromMilliseconds(10)));
        rFunctions.RegisterAction(
            functionId.TypeId,
            void(string param) => { }
        );

        await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded));
        stopWatch.Elapsed.ShouldBeLessThan(TimeSpan.FromMilliseconds(500));
    }
}