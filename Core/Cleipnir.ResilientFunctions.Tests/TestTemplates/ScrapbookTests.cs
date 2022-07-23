using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ParameterSerialization;
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
    public async Task SunshineScenario(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            scrapbookType: typeof(Scrapbook).SimpleQualifiedName(),
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();
            
        var scrapbook = new Scrapbook();
        scrapbook.Initialize(FunctionId, store, DefaultSerializer.Instance, 0);

        var storedScrapbook = (await store.GetFunction(FunctionId))!.Scrapbook;
        storedScrapbook.ShouldNotBeNull();
        storedScrapbook.ScrapbookType.ShouldBe(typeof(Scrapbook).SimpleQualifiedName());
        storedScrapbook.ScrapbookJson.ShouldBeNull();

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

    public abstract Task ScrapbookIsNotUpdatedWhenVersionStampIsNotAsExpected();
    public async Task ScrapbookIsNotUpdatedWhenVersionStampIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            scrapbookType: typeof(Scrapbook).SimpleQualifiedName(),
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();
        await store.TryToBecomeLeader(
            FunctionId,
            Status.Executing,
            expectedEpoch: 0, newEpoch: 1,
            crashedCheckFrequency: 100
        ).ShouldBeTrueAsync();
            
        var scrapbook = new Scrapbook() {Name = "Peter"};
        scrapbook.Initialize(FunctionId, store, DefaultSerializer.Instance,1);
        await scrapbook.Save();
            
        scrapbook = new Scrapbook() {Name = "Ole"};
        scrapbook.Initialize(FunctionId, store, DefaultSerializer.Instance,0);
        await Should.ThrowAsync<ScrapbookSaveFailedException>(scrapbook.Save);
            
        (await store.GetFunction(FunctionId))!
            .Scrapbook!
            .ScrapbookJson!
            .DeserializeFromJsonTo<Scrapbook>()!
            .Name!
            .ShouldBe("Peter");
    }
    
    public abstract Task ScrapbookIsUsedWhenSpecifiedAtRegistration();
    public async Task ScrapbookIsUsedWhenSpecifiedAtRegistration(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var rFunctions = new RFunctions(store);
        var functionId = new FunctionId(
            functionTypeId: nameof(ScrapbookIsUsedWhenSpecifiedAtRegistration),
            functionInstanceId: "instance"
        );
        var synced = new Synced<ParentScrapbook>();
        var rAction = rFunctions.RegisterAction<string, ParentScrapbook>(
            functionId.TypeId,
            (_, scrapbook) => synced.Value = scrapbook,
            scrapbookFactory: () => new ChildScrapbook()
        ).Invoke;

        await rAction("instance", "param");

        await BusyWait.UntilAsync(() => synced.Value != null);
    }

    private class ParentScrapbook : RScrapbook { }
    private class ChildScrapbook : ParentScrapbook { }
}