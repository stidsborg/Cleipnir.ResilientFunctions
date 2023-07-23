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
        var functionId = TestFunctionId.Create();
        await store.CreateFunction(
            functionId,
            param: new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            new StoredScrapbook(new Scrapbook().ToJson(), typeof(Scrapbook).SimpleQualifiedName()),
            leaseExpiration: DateTime.UtcNow.Ticks
        );

        var flag = new SyncedFlag();
        using var rFunctions = new RFunctions(store, new Settings(signOfLifeFrequency: TimeSpan.FromMilliseconds(100)));
        _ = rFunctions.RegisterAction(
            functionId.TypeId,
            void(string param) => flag.Raise()
        );

        await flag.WaitForRaised();

        await BusyWait.Until(
            () => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded)
        );
    }

    public abstract Task CreatedActionWithScrapbookIsCompletedByWatchdog();
    protected async Task CreatedActionWithScrapbookIsCompletedByWatchdog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        await store.CreateFunction(
            functionId,
            param: new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            new StoredScrapbook(new Scrapbook().ToJson(), typeof(Scrapbook).SimpleQualifiedName()),
            leaseExpiration: DateTime.UtcNow.Ticks
        );

        var flag = new SyncedFlag();
        using var rFunctions = new RFunctions(store, new Settings(signOfLifeFrequency: TimeSpan.FromMilliseconds(100)));
        _ = rFunctions.RegisterAction<string, Scrapbook>(
            functionId.TypeId,
            void(string param, Scrapbook scrapbook) => flag.Raise()
        );

        await flag.WaitForRaised();

        await BusyWait.Until(
            () => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded)
        );
        var scrapbook = await store.GetFunction(functionId).Map(sf => sf?.Scrapbook);
        scrapbook.ShouldNotBeNull();
        scrapbook.ScrapbookJson.ShouldNotBeNull();
    }

    public abstract Task CreatedFuncIsCompletedByWatchdog();
    public async Task CreatedFuncIsCompletedByWatchdog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        await store.CreateFunction(
            functionId,
            param: new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            new StoredScrapbook(new Scrapbook().ToJson(), typeof(Scrapbook).SimpleQualifiedName()),
            leaseExpiration: DateTime.UtcNow.Ticks
        );

        var flag = new SyncedFlag();
        using var rFunctions = new RFunctions(store, new Settings(signOfLifeFrequency: TimeSpan.FromMilliseconds(100)));
        _ = rFunctions.RegisterFunc(
            functionId.TypeId,
            string (string param) =>
            {
                flag.Raise();
                return param.ToUpper();
            });

        await flag.WaitForRaised();

        await BusyWait.Until(
            () => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded)
        );
        var resultJson = await store.GetFunction(functionId).Map(sf => sf?.Result?.ResultJson);
        resultJson.ShouldNotBeNull();
        JsonConvert.DeserializeObject<string>(resultJson).ShouldBe("HELLO WORLD");
    }

    public abstract Task CreatedFuncWithScrapbookIsCompletedByWatchdog();
    protected async Task CreatedFuncWithScrapbookIsCompletedByWatchdog(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var functionId = TestFunctionId.Create();
        await store.CreateFunction(
            functionId,
            param: new StoredParameter("hello world".ToJson(), typeof(string).SimpleQualifiedName()),
            new StoredScrapbook(new Scrapbook().ToJson(), typeof(Scrapbook).SimpleQualifiedName()),
            leaseExpiration: DateTime.UtcNow.Ticks
        );

        var flag = new SyncedFlag();
        using var rFunctions = new RFunctions(store, new Settings(signOfLifeFrequency: TimeSpan.FromMilliseconds(100)));
        _ = rFunctions.RegisterFunc(
            functionId.TypeId,
            string (string param, Scrapbook scrapbook) =>
            {
                flag.Raise();
                return param.ToUpper();
            });

        await flag.WaitForRaised();

        await BusyWait.Until(
            () => store.GetFunction(functionId).Map(sf => sf?.Status == Status.Succeeded)
        );
        var scrapbook = await store.GetFunction(functionId).Map(sf => sf?.Scrapbook);
        scrapbook.ShouldNotBeNull();
        scrapbook.ScrapbookJson.ShouldNotBeNull();
        
        var resultJson = await store.GetFunction(functionId).Map(sf => sf?.Result?.ResultJson);
        resultJson.ShouldNotBeNull();
        JsonConvert.DeserializeObject<string>(resultJson).ShouldBe("HELLO WORLD");
    }
    
    private class Scrapbook : RScrapbook {}
}