using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates
{
    public abstract class StoreCrudTests
    {
        private FunctionId FunctionId { get; } = new FunctionId("funcType1", "funcInstance1");
        private TestParameters TestParam { get; } = new TestParameters("Peter", 32);
        private StoredParameter Param => new(TestParam.ToJson(), typeof(TestParameters).SimpleQualifiedName());
        private record TestParameters(string Name, int Age);

        private class TestScrapbook : RScrapbook
        {
            public string? Note { get; set; }
        }
        
        public abstract Task FunctionCanBeCreatedWithASingleParameterSuccessfully();
        public async Task FunctionCanBeCreatedWithASingleParameterSuccessfully(IFunctionStore store)
        {
            await store.CreateFunction(
                FunctionId,
                Param,
                scrapbookType: null,
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            var stored = await store.GetFunction(FunctionId);
            stored!.FunctionId.ShouldBe(FunctionId);
            stored.Parameter.ParamJson.ShouldBe(Param.ParamJson);
            stored.Parameter.ParamType.ShouldBe(Param.ParamType);
            stored.Scrapbook.ShouldBeNull();
            stored.Result.ShouldBeNull();
            stored.Status.ShouldBe(Status.Executing);
            stored.PostponedUntil.ShouldBeNull();
            stored.Epoch.ShouldBe(0);
            stored.SignOfLife.ShouldBe(0);
        }

        public abstract Task FunctionCanBeCreatedWithATwoParametersSuccessfully();
        public async Task FunctionCanBeCreatedWithATwoParametersSuccessfully(IFunctionStore store)
        {
            await store.CreateFunction(
                FunctionId,
                Param,
                scrapbookType: null,
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            var stored = await store.GetFunction(FunctionId);
            stored!.FunctionId.ShouldBe(FunctionId);
            stored.Parameter.ParamJson.ShouldBe(Param.ParamJson);
            stored.Parameter.ParamType.ShouldBe(Param.ParamType);
            stored.Scrapbook.ShouldBeNull();
            stored.Result.ShouldBeNull();
            stored.Status.ShouldBe(Status.Executing);
            stored.PostponedUntil.ShouldBeNull();
            stored.Epoch.ShouldBe(0);
            stored.SignOfLife.ShouldBe(0);
        }
        
        public abstract Task FunctionCanBeCreatedWithATwoParametersAndScrapbookTypeSuccessfully();
        public async Task DoubleParameterWithScrapbookSunshineScenario(IFunctionStore store)
        {
            await store.CreateFunction(
                FunctionId,
                Param,
                scrapbookType: typeof(TestScrapbook).SimpleQualifiedName(),
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            var stored = await store.GetFunction(FunctionId);
            stored!.FunctionId.ShouldBe(FunctionId);
            stored.Parameter.ParamJson.ShouldBe(Param.ParamJson);
            stored.Parameter.ParamType.ShouldBe(Param.ParamType);
            stored.Scrapbook.ShouldNotBeNull();
            stored.Scrapbook.ScrapbookJson.ShouldBeNull();
            stored.Scrapbook.ScrapbookType.ShouldBe(typeof(TestScrapbook).SimpleQualifiedName());
            stored.Result.ShouldBeNull();
            stored.Status.ShouldBe(Status.Executing);
            stored.PostponedUntil.ShouldBeNull();
            stored.Epoch.ShouldBe(0);
            stored.SignOfLife.ShouldBe(0);
        }

        public abstract Task FetchingNonExistingFunctionReturnsNull();
        public async Task FetchingNonExistingFunctionReturnsNull(IFunctionStore store)
            => await store.GetFunction(FunctionId).ShouldBeNullAsync(); 
        
        public abstract Task SignOfLifeIsNotUpdatedWhenItIsNotAsExpected();
        public async Task SignOfLifeIsNotUpdatedWhenItIsNotAsExpected(IFunctionStore store)
        {
            await store.CreateFunction(
                FunctionId,
                Param,
                scrapbookType: null,
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            await store.UpdateSignOfLife(FunctionId, expectedEpoch: 0, newSignOfLife: 1).ShouldBeTrueAsync();

            var storedFunction = await store.GetFunction(FunctionId);
            storedFunction!.Epoch.ShouldBe(0);
        }

        public abstract Task UpdateScrapbookSunshineScenario();
        public async Task UpdateScrapbookSunshineScenario(IFunctionStore store)
        {
            await store.CreateFunction(
                FunctionId,
                Param,
                scrapbookType: typeof(TestScrapbook).SimpleQualifiedName(),
                initialStatus: Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            var scrapbook = new TestScrapbook { Note = "something is still something" };
            await store.SetFunctionState(
                FunctionId,
                Status.Executing,
                scrapbook.ToJson(),
                result: null,
                failed: null,
                postponedUntil: null,
                expectedEpoch: 0
            ).ShouldBeTrueAsync();

            var storedFunction = await store.GetFunction(FunctionId);
            storedFunction!.Scrapbook.ShouldNotBeNull();
            var (scrapbookJson, scrapbookType) = storedFunction.Scrapbook;
            
            scrapbookType.ShouldBe(typeof(TestScrapbook).SimpleQualifiedName());
            scrapbookJson.ShouldBe(scrapbook.ToJson());
        }
        
        public abstract Task ScrapbookUpdateFailsWhenEpochIsNotAsExpected();
        public async Task ScrapbookUpdateFailsWhenEpochIsNotAsExpected(IFunctionStore store)
        {
            await store.CreateFunction(
                FunctionId,
                Param,
                scrapbookType: typeof(TestScrapbook).SimpleQualifiedName(),
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            var scrapbook = new TestScrapbook { Note = "something is still something" };
            await store.SetFunctionState(
                FunctionId,
                Status.Executing,
                scrapbook.ToJson(),
                result: null,
                failed: null,
                postponedUntil: null,
                expectedEpoch: 1
            ).ShouldBeFalseAsync();

            var (scrapbookJson, scrapbookType) = (await store.GetFunction(FunctionId))!.Scrapbook!;
            scrapbookType.ShouldBe(typeof(TestScrapbook).SimpleQualifiedName());
            scrapbookJson.ShouldBeNull();
        }

        public abstract Task GetFunctionsWithStatusOnlyReturnsSucceededFunction();
        public async Task GetFunctionsWithStatusOnlyReturnsSucceededFunction(IFunctionStore store)
        {
            const string functionType = "someFunctionType";
            var function1Id = new FunctionId(functionType, "instance1");
            await store.CreateFunction(
                function1Id,
                Param,
                scrapbookType: null,
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();
            
            await store.SetFunctionState(
                function1Id,
                Status.Succeeded,
                scrapbookJson: null,
                new StoredResult("some result".ToJson(), typeof(string).SimpleQualifiedName()),
                failed: null,
                postponedUntil: null,
                expectedEpoch: 0
            ).ShouldBeTrueAsync();

            var function2Id = new FunctionId(functionType, "instance2");
            await store.CreateFunction(
                function2Id,
                Param,
                scrapbookType: null,
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            );

            var nonCompletes = await store.GetFunctionsWithStatus(
                    functionType.ToFunctionTypeId(),
                    Status.Succeeded,
                    expiresBefore: null
            ).ToTaskList();
            
            nonCompletes.Count.ShouldBe(1);
            var nonCompleted = nonCompletes.Single();
            nonCompleted.InstanceId.Value.ShouldBe("instance1");
            nonCompleted.Status.ShouldBe(Status.Succeeded);
            nonCompleted.SignOfLife.ShouldBe(0);
            nonCompleted.Epoch.ShouldBe(0);
            nonCompleted.PostponedUntil.ShouldBeNull();
        }
    }
}