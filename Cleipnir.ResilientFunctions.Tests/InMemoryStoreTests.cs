using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests
{
    [TestClass]
    public class InMemoryStoreTests
    {
        private FunctionId FunctionId { get; } = new FunctionId("functionId", "instanceId");
        
        private const string Param = "param";
        
        [TestMethod]
        public async Task SunshineScenarioTest()
        {
            var store = new InMemoryFunctionStore();
            
            var paramJson = Param.ToJson();
            var paramType = Param.GetType().SimpleQualifiedName();
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

        [TestMethod]
        public async Task SignOfLifeIsUpdatedWhenAsExpected()
        {
            var store = new InMemoryFunctionStore();
            
            var paramJson = Param.ToJson();
            var paramType = Param.GetType().SimpleQualifiedName();
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
        
        [TestMethod]
        public async Task SignOfLifeIsNotUpdatedWhenNotAsExpected()
        {
            var store = new InMemoryFunctionStore();
            
            var paramJson = Param.ToJson();
            var paramType = Param.GetType().SimpleQualifiedName();
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