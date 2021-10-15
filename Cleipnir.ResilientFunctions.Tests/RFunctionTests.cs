using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;
using static Cleipnir.ResilientFunctions.Tests.Utils.TestUtils;
using static Cleipnir.ResilientFunctions.Utils.Helpers;

namespace Cleipnir.ResilientFunctions.Tests
{
    [TestClass]
    public class RFunctionTests
    {
        private readonly FunctionTypeId _functionTypeId = "functionId".ToFunctionTypeId();

        [TestMethod]
        public async Task SunshineScenario()
        {
            var store = new InMemoryFunctionStore();

            async Task<string> ToUpper(string s)
            {
                await Task.Delay(10);
                return s.ToUpper();
            }

            var rFunctions = RFunctions.Create(store);

            var rFunc = rFunctions.Register(
                _functionTypeId, 
                default(string), 
                ToUpper
            );
            
            var result = await rFunc("hello");
            result.ShouldBe("HELLO");

            var instanceId = HashHelper.SHA256Hash("hello".ToJson()).ToFunctionInstanceId();
            var storeResult = await store.GetFunctionResult(new FunctionId(_functionTypeId, instanceId));
            storeResult.ShouldNotBeNull();
            storeResult.Deserialize().ShouldBe("HELLO");
        }
        
        [TestMethod]
        public async Task NonCompletedFunctionIsCompletedByWatchDog()
        {
            var store = new InMemoryFunctionStore();
            var param = "test";
            var instanceId = GenerateInstanceIdFrom(param).ToFunctionInstanceId();

            var throwingFunctionWrapper = new FunctionWrapper(true);
            var nonCompletingRFunctions = RFunctions
                .Create(store, TimeSpan.Zero)
                .Register(
                    _functionTypeId, 
                    default(string), 
                    throwingFunctionWrapper.Method
                );

            SafeTry(() => _ = nonCompletingRFunctions(param));

            var nonThrowingFunctionWrapper = new FunctionWrapper(false);
            using var rFunctions = RFunctions.Create(
                store, 
                TimeSpan.FromMilliseconds(2)
            );

            var rFunc = rFunctions.Register(
                _functionTypeId, 
                default(string), 
                nonThrowingFunctionWrapper.Method
            );

            await BusyWait.Until(
                async () => await store.GetFunctionResult(new FunctionId(_functionTypeId, instanceId)) != null
            );

            (await rFunc(param)).ShouldBe("TEST");
        }

        private class FunctionWrapper
        {
            private readonly bool _shouldThrow;

            public FunctionWrapper(bool shouldThrow) => _shouldThrow = shouldThrow;

            public Task<string> Method(string s)
            {
                if (_shouldThrow)
                    throw new Exception();

                return Task.FromResult(s.ToUpper());
            }
        }
    }
}