using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests
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
            var initialTimeOfLife = DateTime.UtcNow.Ticks;

            await store.StoreFunction(
                FunctionId,
                paramJson,
                paramType,
                initialTimeOfLife    
            );

            var nonCompletes = await store
                .GetNonCompletedFunctions(FunctionId.TypeId, initialTimeOfLife + 10)
                .ToTaskList();
            
            nonCompletes.Count.ShouldBe(1);
            var nonCompleted = nonCompletes[0];
            nonCompleted.FunctionId.ShouldBe(FunctionId);
            nonCompleted.ParamJson.ShouldBe(paramJson);
            nonCompleted.ParamType.ShouldBe(paramType);
            nonCompleted.SignOfLife.ShouldBe(initialTimeOfLife);

            store.GetFunctionResult(FunctionId).Result.ShouldBeNull();
            
            var result = "world";
            var resultJson = result.ToJson();
            var resultType = result.GetType().SimpleQualifiedName();
            await store.StoreFunctionResult(FunctionId, resultJson, resultType);
            
            var storeResult = await store.GetFunctionResult(FunctionId);
            storeResult.ShouldNotBeNull();
            storeResult.Deserialize().ShouldBe("world");
        }

        public abstract Task SignOfLifeIsUpdatedWhenAsExpected();
        public async Task SignOfLifeIsUpdatedWhenAsExpected(IFunctionStore store)
        {
            var paramJson = PARAM.ToJson();
            var paramType = PARAM.GetType().SimpleQualifiedName();
            var initialTimeOfLife = DateTime.UtcNow.Ticks;

            await store.StoreFunction(
                FunctionId,
                paramJson,
                paramType,
                initialTimeOfLife    
            );

            var success = await store.UpdateSignOfLife(
                FunctionId,  
                initialTimeOfLife, 
                initialTimeOfLife + 2
            );

            success.ShouldBeTrue();
            var nonCompletedFunctions = await store.GetNonCompletedFunctions(FunctionId.TypeId, initialTimeOfLife + 1);
            nonCompletedFunctions.ShouldBeEmpty();
            
            nonCompletedFunctions = await store.GetNonCompletedFunctions(FunctionId.TypeId, initialTimeOfLife + 3);
            nonCompletedFunctions.Single().SignOfLife.ShouldBe(initialTimeOfLife + 2);
        }

        public abstract Task SignOfLifeIsNotUpdatedWhenNotAsExpected();
        public async Task SignOfLifeIsNotUpdatedWhenNotAsExpected(IFunctionStore store)
        {
            var paramJson = PARAM.ToJson();
            var paramType = PARAM.GetType().SimpleQualifiedName();
            var initialTimeOfLife = DateTime.UtcNow.Ticks;

            await store.StoreFunction(
                FunctionId,
                paramJson,
                paramType,
                initialTimeOfLife    
            );

            var success = await store.UpdateSignOfLife(
                FunctionId,  
                initialTimeOfLife + 1, 
                initialTimeOfLife + 2
            );

            success.ShouldBeFalse();
            var nonCompletedFunctions = await store.GetNonCompletedFunctions(FunctionId.TypeId, initialTimeOfLife + 1);
            nonCompletedFunctions.Single().SignOfLife.ShouldBe(initialTimeOfLife);
        }
    }
}