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
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100
        );
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        using var rFunctions = new RFunctions(store, new Settings(
            crashedCheckFrequency: TimeSpan.FromMilliseconds(10),
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
            new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100
        );
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        using var rFunctions = new RFunctions(store, new Settings(crashedCheckFrequency: TimeSpan.FromMilliseconds(10)));
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
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100
        );
        await store.PostponeFunction(
            functionId,
            postponeUntil: 0,
            scrapbookJson: new RScrapbook().ToJson(),
            expectedEpoch: 0
        ).ShouldBeTrueAsync();

        var stopWatch = new Stopwatch();
        stopWatch.Start();
        using var rFunctions = new RFunctions(store, new Settings(
            postponedCheckFrequency: TimeSpan.FromMilliseconds(10),
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

        var functionId = new FunctionId("FunctionTypeId", "FunctionInstanceId");
        await store.CreateFunction(
            functionId,
            new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100
        );
        await store.PostponeFunction(
            functionId,
            postponeUntil: 0,
            scrapbookJson: new RScrapbook().ToJson(),
            expectedEpoch: 0
        );

        var stopWatch = new Stopwatch();
        stopWatch.Start();
        using var rFunctions = new RFunctions(store, new Settings(postponedCheckFrequency: TimeSpan.FromMilliseconds(10)));
        rFunctions.RegisterAction(
            functionId.TypeId,
            void(string param) => { }
        );

        await BusyWait.Until(() => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded));
        stopWatch.Elapsed.ShouldBeLessThan(TimeSpan.FromMilliseconds(500));
    }
}