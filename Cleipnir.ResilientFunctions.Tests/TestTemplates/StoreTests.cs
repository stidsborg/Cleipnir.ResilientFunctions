using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates
{
    public abstract class StoreTests
    {
        private FunctionId FunctionId { get; } = new FunctionId("functionId", "instanceId");
        private const string PARAM = "param";

        public abstract Task SunshineScenarioTest();
        public async Task SunshineScenarioTest(IFunctionStore store)
        {
            var paramJson = PARAM.ToJson();
            var paramType = PARAM.GetType().SimpleQualifiedName();

            await store.CreateFunction(
                FunctionId,
                param: new StoredParameter(paramJson, paramType),
                scrapbookType: null,
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 1
            ).ShouldBeTrueAsync();

            var nonCompletes = await store
                .GetFunctionsWithStatus(FunctionId.TypeId, Status.Executing)
                .ToTaskList();
            
            nonCompletes.Count.ShouldBe(1);
            var nonCompleted = nonCompletes[0];
            nonCompleted.InstanceId.ShouldBe(FunctionId.InstanceId);
            nonCompleted.Status.ShouldBe(Status.Executing);
            nonCompleted.Epoch.ShouldBe(0);
            nonCompleted.SignOfLife.ShouldBe(1);
            nonCompleted.PostponedUntil.ShouldBeNull();

            var storedFunction = await store.GetFunction(FunctionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.FunctionId.ShouldBe(FunctionId);
            storedFunction.Parameter.ParamJson.ShouldBe(paramJson);
            storedFunction.Parameter.ParamType.ShouldBe(paramType);
            storedFunction.Scrapbook.ShouldBeNull();
            storedFunction.Epoch.ShouldBe(0);
            storedFunction.SignOfLife.ShouldBe(1);
            storedFunction.PostponedUntil.ShouldBeNull();

            const string result = "hello world";
            var resultJson = result.ToJson();
            var resultType = result.GetType().SimpleQualifiedName();
            await store.SetFunctionState(
                FunctionId,
                Status.Succeeded,
                scrapbookJson: null,
                result: new StoredResult(resultJson, resultType),
                failed: null,
                postponedUntil: null,
                expectedEpoch: 0
            ).ShouldBeTrueAsync();
            
            storedFunction = await store.GetFunction(FunctionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Result.ShouldNotBeNull();
            storedFunction.Result.Deserialize(DefaultSerializer.Instance).ShouldBe(result);
        }

        public abstract Task SignOfLifeIsUpdatedWhenAsExpected();
        public async Task SignOfLifeIsUpdatedWhenAsExpected(IFunctionStore store)
        {
            var paramJson = PARAM.ToJson();
            var paramType = PARAM.GetType().SimpleQualifiedName();

            await store.CreateFunction(
                FunctionId,
                param: new StoredParameter(paramJson, paramType),
                scrapbookType: null,
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            await store
                .UpdateSignOfLife(FunctionId, expectedEpoch: 0, newSignOfLife: 1)
                .ShouldBeTrueAsync();

            var nonCompletedFunctions = 
                await store.GetFunctionsWithStatus(FunctionId.TypeId, Status.Executing);
            var nonCompletedFunction = nonCompletedFunctions.Single();
            nonCompletedFunction.Epoch.ShouldBe(0);
            nonCompletedFunction.SignOfLife.ShouldBe(1);
        }

        public abstract Task SignOfLifeIsNotUpdatedWhenNotAsExpected();
        public async Task SignOfLifeIsNotUpdatedWhenNotAsExpected(IFunctionStore store)
        {
            var paramJson = PARAM.ToJson();
            var paramType = PARAM.GetType().SimpleQualifiedName();

            await store.CreateFunction(
                FunctionId,
                param: new StoredParameter(paramJson, paramType),
                scrapbookType: null,
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            await store.UpdateSignOfLife(
                FunctionId,  
                expectedEpoch: 1,
                newSignOfLife: 1
            ).ShouldBeFalseAsync();

            var nonCompletedFunctions = 
                await store.GetFunctionsWithStatus(FunctionId.TypeId, Status.Executing);
            var nonCompletedFunction = nonCompletedFunctions.Single();
            nonCompletedFunction.Epoch.ShouldBe(0);
            nonCompletedFunction.SignOfLife.ShouldBe(0);
        }
        
        public abstract Task BecomeLeaderSucceedsWhenEpochIsAsExpected();
        public async Task BecomeLeaderSucceedsWhenEpochIsAsExpected(IFunctionStore store)
        {
            var paramJson = PARAM.ToJson();
            var paramType = PARAM.GetType().SimpleQualifiedName();

            await store.CreateFunction(
                FunctionId,
                param: new StoredParameter(paramJson, paramType),
                scrapbookType: null,
                Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            await store
                .TryToBecomeLeader(FunctionId, Status.Executing, 0, 1)
                .ShouldBeTrueAsync();

            var storedFunction = await store.GetFunction(FunctionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Epoch.ShouldBe(1);
            storedFunction.SignOfLife.ShouldBe(0);
        }
        
        public abstract Task BecomeLeaderFailsWhenEpochIsNotAsExpected();
        public async Task BecomeLeaderFailsWhenEpochIsNotAsExpected(IFunctionStore store)
        {
            var paramJson = PARAM.ToJson();
            var paramType = PARAM.GetType().SimpleQualifiedName();

            await store.CreateFunction(
                FunctionId,
                param: new StoredParameter(paramJson, paramType),
                scrapbookType: null,
                Status.Executing,
                initialEpoch: 2,
                initialSignOfLife: 0
            ).ShouldBeTrueAsync();

            await store
                .TryToBecomeLeader(FunctionId, Status.Executing, 0, 1)
                .ShouldBeFalseAsync();

            var storedFunction = await store.GetFunction(FunctionId);
            storedFunction.ShouldNotBeNull();
            storedFunction.Epoch.ShouldBe(2);
            storedFunction.SignOfLife.ShouldBe(0);
        }
    }
}