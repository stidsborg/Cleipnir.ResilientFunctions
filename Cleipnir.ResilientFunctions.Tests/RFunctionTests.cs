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
    public abstract class RFunctionTests
    {
        private readonly FunctionTypeId _functionTypeId = "functionId".ToFunctionTypeId();
        
        public abstract Task SunshineScenario();
        public async Task SunshineScenario(IFunctionStore store)
        {
            await SunshineScenario(store, null);
            await SunshineScenario(store, "id");
        }
        
        private async Task SunshineScenario(IFunctionStore store, string? functionInstanceId)
        {
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
            
            var result = await rFunc("hello", functionInstanceId?.ToFunctionInstanceId());
            result.ShouldBe("HELLO");

            var instanceIfWhenNoneProvided = HashHelper.SHA256Hash("hello".ToJson()).ToFunctionInstanceId();
            var storeResult = await store.GetFunctionResult(
                new FunctionId(
                    _functionTypeId, 
                    functionInstanceId?.ToFunctionInstanceId() ?? instanceIfWhenNoneProvided
                )
            );
            storeResult.ShouldNotBeNull();
            storeResult.Deserialize().ShouldBe("HELLO");
        }

        public abstract Task NonCompletedFunctionIsCompletedByWatchDog();
        public async Task NonCompletedFunctionIsCompletedByWatchDog(IFunctionStore store)
        {
            await NonCompletedFunctionIsCompletedByWatchDog(store, null);
            await NonCompletedFunctionIsCompletedByWatchDog(store, "id");
        }
        
        private async Task NonCompletedFunctionIsCompletedByWatchDog(IFunctionStore store, string? functionInstanceId)
        {
            const string param = "test";

            var throwingFunctionWrapper = new FunctionWrapper(true);
            var nonCompletingRFunctions = RFunctions
                .Create(store, TimeSpan.Zero)
                .Register(
                    _functionTypeId, 
                    default(string), 
                    throwingFunctionWrapper.Method
                );

            SafeTry(() => _ = nonCompletingRFunctions(param, functionInstanceId?.ToFunctionInstanceId()));

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

            var functionId = new FunctionId(
                _functionTypeId,
                functionInstanceId?.ToFunctionInstanceId() ?? GenerateFunctionInstanceIdFrom(param)
            );
            await BusyWait.Until(async () => await store.GetFunctionResult(functionId) != null);
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