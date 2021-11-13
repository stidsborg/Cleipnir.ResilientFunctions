using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests
{
    public abstract class ScrapbookTests
    {
        private FunctionId FunctionId { get; } = new FunctionId("typeId", "instanceId");
        private Parameter Param1 { get; } = new Parameter(
            ParamJson: "HelloWorld".ToJson(),
            ParamType: typeof(string).SimpleQualifiedName()
        );
        
        private class Scrapbook : RScrapbook
        {
            public string? Name { get; set; }
        }
        
        public abstract Task SunshineScenario();
        public async Task SunshineScenario(IFunctionStore store)
        {
            await store.StoreFunction(
                FunctionId,
                Param1,
                param2: null,
                scrapbookType: typeof(Scrapbook).SimpleQualifiedName(),
                initialSignOfLife: 100
            );
            
            var scrapbook = new Scrapbook();
            scrapbook.Initialize(FunctionId, store, 0);

            var storedScrapbook = (await store.GetFunction(FunctionId))!.Scrapbook;
            storedScrapbook.ShouldNotBeNull();
            storedScrapbook.ScrapbookType.ShouldBe(typeof(Scrapbook).SimpleQualifiedName());
            storedScrapbook.ScrapbookJson.ShouldBeNull();

            await scrapbook.Save();

            storedScrapbook = (await store.GetFunction(FunctionId))!.Scrapbook;
            storedScrapbook.ShouldNotBeNull();
            storedScrapbook.ScrapbookType.ShouldBe(typeof(Scrapbook).SimpleQualifiedName());
            storedScrapbook.ScrapbookJson.ShouldNotBeNull();
            storedScrapbook.ScrapbookJson!.FromJsonTo<Scrapbook>()!.Name.ShouldBeNull(); 
            
            scrapbook.Name = "Peter"; 
            await scrapbook.Save();
            
            storedScrapbook = (await store.GetFunction(FunctionId))!.Scrapbook;
            storedScrapbook.ShouldNotBeNull();
            storedScrapbook.ScrapbookType.ShouldBe(typeof(Scrapbook).SimpleQualifiedName());
            storedScrapbook.ScrapbookJson.ShouldNotBeNull();
            storedScrapbook.ScrapbookJson!.FromJsonTo<Scrapbook>()!.Name.ShouldBe("Peter");
            
            scrapbook.Name = "Ole"; 
            await scrapbook.Save();
            
            storedScrapbook = (await store.GetFunction(FunctionId))!.Scrapbook;
            storedScrapbook.ShouldNotBeNull();
            storedScrapbook.ScrapbookType.ShouldBe(typeof(Scrapbook).SimpleQualifiedName());
            storedScrapbook.ScrapbookJson.ShouldNotBeNull();
            storedScrapbook.ScrapbookJson!.FromJsonTo<Scrapbook>()!.Name.ShouldBe("Ole");
        }

        public abstract Task ScrapbookIsNotUpdatedWhenVersionStampIsNotAsExpected();
        public async Task ScrapbookIsNotUpdatedWhenVersionStampIsNotAsExpected(IFunctionStore store)
        {
            await store.StoreFunction(
                FunctionId,
                Param1,
                param2: null,
                scrapbookType: typeof(Scrapbook).SimpleQualifiedName(),
                initialSignOfLife: 100
            );
            
            var scrapbook = new Scrapbook() {Name = "Peter"};
            scrapbook.Initialize(FunctionId, store, 0);
            await scrapbook.Save();
            
            scrapbook = new Scrapbook() {Name = "Ole"};
            scrapbook.Initialize(FunctionId, store, 0);
            await Should.ThrowAsync<ScrapbookSaveFailedException>(scrapbook.Save);
            
            (await store.GetFunction(FunctionId))!
                .Scrapbook!
                .ScrapbookJson!
                .FromJsonTo<Scrapbook>()!
                .Name!
                .ShouldBe("Peter");
        }
    }
}