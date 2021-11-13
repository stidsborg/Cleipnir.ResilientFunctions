using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests
{
    public abstract class StoreCrudTests
    {
        private FunctionId FunctionId { get; } = new FunctionId("funcType1", "funcInstance1");
        private TestParam TestParam1 { get; } = new TestParam("Peter", 32);
        private Parameter Param1 => new(TestParam1.ToJson(), typeof(TestParam).SimpleQualifiedName());
        private TestParam TestParam2 { get; } = new TestParam("Pia", 29);
        private Parameter Param2 => new(TestParam2.ToJson(), typeof(TestParam).SimpleQualifiedName());
        private record TestParam(string Name, int Age);

        private class TestScrapbook : RScrapbook
        {
            public string? Note { get; set; }
        }
        
        public abstract Task SingleParameterSunshineScenario();
        public async Task SingleParameterSunshineScenario(IFunctionStore store)
        {
            await store.StoreFunction(
                FunctionId,
                Param1,
                null,
                null,
                0
            );

            var stored = await store.GetFunction(FunctionId);
            stored!.FunctionId.ShouldBe(FunctionId);
            stored.Parameter1.ParamJson.ShouldBe(Param1.ParamJson);
            stored.Parameter1.ParamType.ShouldBe(Param1.ParamType);
            stored.Result.ShouldBeNull();
            stored.Scrapbook.ShouldBeNull();
            stored.SignOfLife.ShouldBe(0);

            (await store.GetFunctionResult(FunctionId)).ShouldBeNull();
            
            var success = await store.UpdateSignOfLife(FunctionId, 0, 1);
            success.ShouldBeTrue();
            
            (await store.GetFunction(FunctionId))!.SignOfLife.ShouldBe(1);
            
            await store.StoreFunctionResult(
                FunctionId,
                "some result".ToJson(),
                typeof(string).SimpleQualifiedName()
            );

            stored = await store.GetFunction(FunctionId);
            stored!.Result.ShouldNotBeNull();
            stored.Result.ResultJson.ShouldBe("some result".ToJson());
            stored.Result.ResultType.ShouldBe(typeof(string).SimpleQualifiedName());
        }

        public abstract Task DoubleParameterSunshineScenario();
        public async Task DoubleParameterSunshineScenario(IFunctionStore store)
        {
            await store.StoreFunction(
                FunctionId,
                Param1,
                Param2,
                null,
                0
            );

            var stored = await store.GetFunction(FunctionId);
            stored!.FunctionId.ShouldBe(FunctionId);
            stored.Parameter1.ParamJson.ShouldBe(Param1.ParamJson);
            stored.Parameter1.ParamType.ShouldBe(Param1.ParamType);
            stored.Parameter2!.ParamJson.ShouldBe(Param2.ParamJson);
            stored.Parameter2.ParamType.ShouldBe(Param2.ParamType);
            stored.Result.ShouldBeNull();
            stored.Scrapbook.ShouldBeNull();
            stored.SignOfLife.ShouldBe(0);

            (await store.GetFunctionResult(FunctionId)).ShouldBeNull();
            
            var success = await store.UpdateSignOfLife(FunctionId, 0, 1);
            success.ShouldBeTrue();
            
            (await store.GetFunction(FunctionId))!.SignOfLife.ShouldBe(1);
            
            await store.StoreFunctionResult(
                FunctionId,
                "some result".ToJson(),
                typeof(string).SimpleQualifiedName()
            );

            stored = await store.GetFunction(FunctionId);
            stored!.Result.ShouldNotBeNull();
            stored.Result.ResultJson.ShouldBe("some result".ToJson());
            stored.Result.ResultType.ShouldBe(typeof(string).SimpleQualifiedName());
        }
        
        public abstract Task DoubleParameterWithScrapbookSunshineScenario();
        public async Task DoubleParameterWithScrapbookSunshineScenario(IFunctionStore store)
        {
            await store.StoreFunction(
                FunctionId,
                Param1,
                Param2,
                typeof(TestScrapbook).SimpleQualifiedName(),
                0
            );

            var stored = await store.GetFunction(FunctionId);
            stored!.FunctionId.ShouldBe(FunctionId);
            stored.Parameter1.ParamJson.ShouldBe(Param1.ParamJson);
            stored.Parameter1.ParamType.ShouldBe(Param1.ParamType);
            stored.Parameter2!.ParamJson.ShouldBe(Param2.ParamJson);
            stored.Parameter2.ParamType.ShouldBe(Param2.ParamType);
            stored.Result.ShouldBeNull();
            stored.Scrapbook.ShouldNotBeNull();
            stored.Scrapbook.ScrapbookType.ShouldBe(typeof(TestScrapbook).SimpleQualifiedName());
            stored.Scrapbook.ScrapbookJson.ShouldBeNull();
            stored.Scrapbook.VersionStamp.ShouldBe(0);
            stored.SignOfLife.ShouldBe(0);

            (await store.GetFunctionResult(FunctionId)).ShouldBeNull();
            
            var success = await store.UpdateSignOfLife(FunctionId, 0, 1);
            success.ShouldBeTrue();
            
            (await store.GetFunction(FunctionId))!.SignOfLife.ShouldBe(1);
            
            await store.StoreFunctionResult(
                FunctionId,
                "some result".ToJson(),
                typeof(string).SimpleQualifiedName()
            );

            stored = await store.GetFunction(FunctionId);
            stored!.Result.ShouldNotBeNull();
            stored.Result.ResultJson.ShouldBe("some result".ToJson());
            stored.Result.ResultType.ShouldBe(typeof(string).SimpleQualifiedName());
        }

        public abstract Task FetchingNonExistingFunctionReturnsNull();
        public async Task FetchingNonExistingFunctionReturnsNull(IFunctionStore store)
        {
            (await store.GetFunction(FunctionId)).ShouldBeNull();
        }
        
        public abstract Task SignOfLifeIsNotUpdatedWhenItIsNotAsExpected();
        public async Task SignOfLifeIsNotUpdatedWhenItIsNotAsExpected(IFunctionStore store)
        {
            var param1 = new Parameter(
                new TestParam("Peter", 32).ToJson(),
                typeof(TestParam).SimpleQualifiedName()
            );
            
            await store.StoreFunction(FunctionId, param1, null, null, 0);

            var success = await store.UpdateSignOfLife(FunctionId, 1, 10);
            success.ShouldBeFalse();
            
            (await store.GetFunction(FunctionId))!.SignOfLife.ShouldBe(0);
        }

        public abstract Task UpdateScrapbookSunshineScenario();
        public async Task UpdateScrapbookSunshineScenario(IFunctionStore store)
        {
            var param1 = new Parameter(
                new TestParam("Peter", 32).ToJson(),
                typeof(TestParam).SimpleQualifiedName()
            );

            await store.StoreFunction(
                FunctionId,
                param1,
                null,
                typeof(TestScrapbook).SimpleQualifiedName(),
                0
            );

            var scrapbook = new TestScrapbook { Note = "something is still something" };
            var success = await store.UpdateScrapbook(
                FunctionId,
                scrapbook.ToJson(),
                0,
                1
            );
            success.ShouldBeTrue();

            var storedScrapbook = (await store.GetFunction(FunctionId))!.Scrapbook!;
            storedScrapbook.VersionStamp.ShouldBe(1);
            storedScrapbook.ScrapbookType.ShouldBe(typeof(TestScrapbook).SimpleQualifiedName());
            storedScrapbook.ScrapbookJson.ShouldBe(scrapbook.ToJson());
        }
        
        public abstract Task UpdateScrapbookFailsWhenTimestampIsNotAsExpected();
        public async Task UpdateScrapbookFailsWhenTimestampIsNotAsExpected(IFunctionStore store)
        {
            var param1 = new Parameter(
                new TestParam("Peter", 32).ToJson(),
                typeof(TestParam).SimpleQualifiedName()
            );

            await store.StoreFunction(
                FunctionId,
                param1,
                null,
                typeof(TestScrapbook).SimpleQualifiedName(),
                0
            );

            var scrapbook = new TestScrapbook { Note = "something is still something" };
            var success = await store.UpdateScrapbook(
                FunctionId,
                scrapbook.ToJson(),
                1,
                2
            );
            success.ShouldBeFalse();
            
            var storedScrapbook = (await store.GetFunction(FunctionId))!.Scrapbook!;
            storedScrapbook.VersionStamp.ShouldBe(0);
            storedScrapbook.ScrapbookType.ShouldBe(typeof(TestScrapbook).SimpleQualifiedName());
            storedScrapbook.ScrapbookJson.ShouldBeNull();
        }

        public abstract Task OnlyNonCompletedFunctionsAreReturnedWhenStoreMethodIsInvoked();
        public async Task OnlyNonCompletedFunctionsAreReturnedWhenStoreMethodIsInvoked(IFunctionStore store)
        {
            await store.StoreFunction(FunctionId, Param1, null, null, 0);
            await store.StoreFunctionResult(
                FunctionId,
                "some result".ToJson(),
                typeof(string).SimpleQualifiedName()
            );

            await store.StoreFunction(
                new FunctionId(FunctionId.TypeId.Value, "someInstance"),
                Param1,
                null, null,
                10
            );

            var nonCompletes = await store
                .GetNonCompletedFunctions(FunctionId.TypeId)
                .ToTaskList();
            
            nonCompletes.Count.ShouldBe(1);
            nonCompletes[0].InstanceId.Value.ShouldBe("someInstance");
            nonCompletes[0].LastSignOfLife.ShouldBe(10);
        }
    }
}