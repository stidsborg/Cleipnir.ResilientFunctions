using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;
using static Cleipnir.ResilientFunctions.Tests.Utils.TestUtils;

namespace Cleipnir.ResilientFunctions.Tests
{
    public abstract class RFunctionTests
    {
        private readonly FunctionTypeId _functionTypeId = "functionId".ToFunctionTypeId();
        
        public abstract Task SunshineScenario();

        public async Task SunshineScenario(IFunctionStore store)
        {
            async Task<string> ToUpper(string s)
            {
                await Task.Delay(10);
                return s.ToUpper();
            }

            var unhandledExceptionHandler = new UnhandledExceptionCatcher();

            var rFunctions = RFunctions.Create(store, unhandledExceptionHandler.Catch);

            var rFunc = rFunctions
                .Register(
                    _functionTypeId,
                    (string s) => ToUpper(s),
                    _ => _
                );

            var result = await rFunc("hello");
            result.ShouldBe("HELLO");
            
            var storeResult = await store.GetFunctionResult(
                new FunctionId(
                    _functionTypeId, 
                    "hello".ToFunctionInstanceId()
                )
            );
            storeResult.ShouldNotBeNull();
            storeResult.Deserialize().ShouldBe("HELLO");
            
            unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
        }

        public abstract Task NonCompletedFunctionIsCompletedByWatchDog();

        protected async Task NonCompletedFunctionIsCompletedByWatchDog(IFunctionStore store)
        {
            const string param = "test";
            {
                var throwingFunctionWrapper = new FunctionWrapper(true);
                var unhandledExceptionHandler = new UnhandledExceptionCatcher();
                var nonCompletingRFunctions = RFunctions
                    .Create(store, unhandledExceptionHandler.Catch, TimeSpan.Zero)
                    .Register(
                        _functionTypeId,
                        (string s) => throwingFunctionWrapper.Method(s),
                        _ => _);

                SafeTry(() => _ = nonCompletingRFunctions(param));   
                unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
            }
            {
                var unhandledExceptionHandler = new UnhandledExceptionCatcher();
                var nonThrowingFunctionWrapper = new FunctionWrapper(false);
                using var rFunctions = RFunctions.Create(
                    store, 
                    unhandledExceptionHandler.Catch,
                    TimeSpan.FromMilliseconds(2)
                );

                var rFunc = rFunctions
                    .Register(
                        _functionTypeId,
                        (string s) => nonThrowingFunctionWrapper.Method(s),
                        _ => _
                    );

                var functionId = new FunctionId(_functionTypeId, param.ToFunctionInstanceId());
                await BusyWait.Until(async () => await store.GetFunctionResult(functionId) != null);
                (await rFunc(param)).ShouldBe("TEST");                
                unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
            }
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