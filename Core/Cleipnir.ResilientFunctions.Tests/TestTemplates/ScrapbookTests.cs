﻿using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class ScrapbookTests
{
    private FunctionId FunctionId { get; } = new FunctionId("typeId", "instanceId");
    private StoredParameter Param { get; } = new StoredParameter(
        ParamJson: "HelloWorld".ToJson(),
        ParamType: typeof(string).SimpleQualifiedName()
    );
        
    private class Scrapbook : RScrapbook
    {
        public string? Name { get; set; }
    }
        
    public abstract Task SunshineScenario();
    protected async Task SunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            new StoredScrapbook(new Scrapbook().ToJson(), typeof(Scrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();
            
        var scrapbook = new Scrapbook();
        scrapbook.Initialize(onSave: async () =>
        {
            var (scrapbookJson, _) = DefaultSerializer.Instance.SerializeScrapbook(scrapbook);
            await store.SetScrapbook(
                FunctionId,
                scrapbookJson,
                expectedEpoch: 0
            );
        });

        var storedScrapbook = (await store.GetFunction(FunctionId))!.Scrapbook;
        storedScrapbook.ShouldNotBeNull();
        storedScrapbook.ScrapbookType.ShouldBe(typeof(Scrapbook).SimpleQualifiedName());
        storedScrapbook.ScrapbookJson.ShouldNotBeNull();

        await scrapbook.Save();

        storedScrapbook = (await store.GetFunction(FunctionId))!.Scrapbook;
        storedScrapbook.ShouldNotBeNull();
        storedScrapbook.ScrapbookType.ShouldBe(typeof(Scrapbook).SimpleQualifiedName());
        storedScrapbook.ScrapbookJson.ShouldNotBeNull();
        storedScrapbook.ScrapbookJson!.DeserializeFromJsonTo<Scrapbook>()!.Name.ShouldBeNull(); 
            
        scrapbook.Name = "Peter"; 
        await scrapbook.Save();
            
        storedScrapbook = (await store.GetFunction(FunctionId))!.Scrapbook;
        storedScrapbook.ShouldNotBeNull();
        storedScrapbook.ScrapbookType.ShouldBe(typeof(Scrapbook).SimpleQualifiedName());
        storedScrapbook.ScrapbookJson.ShouldNotBeNull();
        storedScrapbook.ScrapbookJson!.DeserializeFromJsonTo<Scrapbook>()!.Name.ShouldBe("Peter");
            
        scrapbook.Name = "Ole"; 
        await scrapbook.Save();
            
        storedScrapbook = (await store.GetFunction(FunctionId))!.Scrapbook;
        storedScrapbook.ShouldNotBeNull();
        storedScrapbook.ScrapbookType.ShouldBe(typeof(Scrapbook).SimpleQualifiedName());
        storedScrapbook.ScrapbookJson.ShouldNotBeNull();
        storedScrapbook.ScrapbookJson!.DeserializeFromJsonTo<Scrapbook>()!.Name.ShouldBe("Ole");
    }

    public abstract Task ScrapbookIsNotUpdatedWhenEpochIsNotAsExpected();
    protected async Task ScrapbookIsNotUpdatedWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            new StoredScrapbook(new RScrapbook().ToJson(), typeof(RScrapbook).SimpleQualifiedName()),
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();
        await store.TryToBecomeLeader(
            FunctionId,
            Status.Executing,
            expectedEpoch: 0, newEpoch: 1,
            crashedCheckFrequency: 100,
            version: 0
        ).ShouldBeTrueAsync();
            
        var scrapbook = new Scrapbook() {Name = "Peter"};
        scrapbook.Initialize(onSave: async () =>
        {
            var (scrapbookJson, _) = DefaultSerializer.Instance.SerializeScrapbook(scrapbook);
            await store.SetScrapbook(
                FunctionId,
                scrapbookJson,
                expectedEpoch: 1
            );
        });
        await scrapbook.Save();
            
        scrapbook = new Scrapbook() {Name = "Ole"};
        scrapbook.Initialize(onSave: async () =>
        {
            var (scrapbookJson, _) = DefaultSerializer.Instance.SerializeScrapbook(scrapbook);
            await store.SetScrapbook(
                FunctionId,
                scrapbookJson,
                expectedEpoch: 0
            );
        });
        await scrapbook.Save();
            
        (await store.GetFunction(FunctionId))!
            .Scrapbook
            .ScrapbookJson!
            .DeserializeFromJsonTo<Scrapbook>()!
            .Name!
            .ShouldBe("Peter");
    }

    public abstract Task ConcreteScrapbookTypeIsUsedWhenSpecifiedAtRegistration();
    protected async Task ConcreteScrapbookTypeIsUsedWhenSpecifiedAtRegistration(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var rFunctions = new RFunctions(store);
        var functionId = new FunctionId(
            functionTypeId: nameof(ConcreteScrapbookTypeIsUsedWhenSpecifiedAtRegistration),
            functionInstanceId: "instance"
        );
        var synced = new Synced<ParentScrapbook>();
        var rAction = rFunctions.RegisterAction<string, ParentScrapbook>(
            functionId.TypeId,
            (_, scrapbook) => synced.Value = scrapbook
        ).Invoke;

        await rAction("instance", "param", new ChildScrapbook());

        await BusyWait.UntilAsync(() => synced.Value != null);
        synced.Value.ShouldBeOfType<ChildScrapbook>();
    }

    private class ParentScrapbook : RScrapbook { }
    private class ChildScrapbook : ParentScrapbook { }

    public abstract Task ChangesToStateDictionaryArePersisted();
    protected async Task ChangesToStateDictionaryArePersisted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var rFunctions = new RFunctions(store);
        
        var rAction = rFunctions.RegisterAction<string, RScrapbook>(
            nameof(ChangesToStateDictionaryArePersisted),
            (_, scrapbook) => scrapbook.StateDictionary["hello"] = "world"
        ).Invoke;

        await rAction.Invoke("instance", "test");
        var sf = await store.GetFunction(
            new FunctionId(nameof(ChangesToStateDictionaryArePersisted), "instance")
        ).ShouldNotBeNullAsync();

        var scrapbook = sf.Scrapbook.Deserialize<RScrapbook>(DefaultSerializer.Instance);
        scrapbook.StateDictionary.Count.ShouldBe(1);
        scrapbook.StateDictionary["hello"].ShouldBe("world");
    }
}