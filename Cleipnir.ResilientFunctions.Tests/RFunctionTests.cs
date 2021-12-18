using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Cleipnir.ResilientFunctions.Utils;
using Shouldly;
using static Cleipnir.ResilientFunctions.Tests.Utils.TestUtils;

namespace Cleipnir.ResilientFunctions.Tests
{
    public abstract class RFunctionTests
    {
        public abstract Task SunshineScenario();
        public async Task SunshineScenario(IFunctionStore store)
        {
            var functionTypeId = nameof(SunshineScenario).ToFunctionTypeId();
            async Task<RResult<string>> ToUpper(string s)
            {
                await Task.Delay(10);
                return s.ToUpper();
            }

            var unhandledExceptionHandler = new UnhandledExceptionCatcher();

            var rFunctions = RFunctions.Create(store, unhandledExceptionHandler.Catch);

            var rFunc = rFunctions
                .Register(
                    functionTypeId,
                    (string s) => ToUpper(s),
                    _ => _
                );

            var result = await rFunc("hello");
            result.ShouldBe("HELLO");
            
            var storedFunction = await store.GetFunction(
                new FunctionId(
                    functionTypeId, 
                    "hello".ToFunctionInstanceId()
                )
            );
            storedFunction.ShouldNotBeNull();
            storedFunction.Result.ShouldNotBeNull();
            var storedResult = storedFunction.Result.Deserialize().ToString();
            storedResult.ShouldBe("HELLO");
            
            unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
        }

        public abstract Task NonCompletedFunctionIsCompletedByWatchDog();
        protected async Task NonCompletedFunctionIsCompletedByWatchDog(IFunctionStore store)
        {
            var functionTypeId = nameof(NonCompletedFunctionIsCompletedByWatchDog).ToFunctionTypeId();
            const string param = "test";
            {
                var throwingFunctionWrapper = new UncaughtExceptionFunctionWrapper(true);
                var unhandledExceptionHandler = new UnhandledExceptionCatcher();
                var nonCompletingRFunctions = RFunctions
                    .Create(store, unhandledExceptionHandler.Catch, TimeSpan.Zero)
                    .Register(
                        functionTypeId,
                        (string s) => throwingFunctionWrapper.Method(s),
                        _ => _
                    );

                SafeTry(() => _ = nonCompletingRFunctions(param));   
                unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
            }
            {
                var unhandledExceptionHandler = new UnhandledExceptionCatcher();
                var nonThrowingFunctionWrapper = new UncaughtExceptionFunctionWrapper(false);
                using var rFunctions = RFunctions.Create(
                    store, 
                    unhandledExceptionHandler.Catch,
                    TimeSpan.FromMilliseconds(2)
                );

                var rFunc = rFunctions
                    .Register(
                        functionTypeId,
                        (string s) => nonThrowingFunctionWrapper.Method(s),
                        _ => _
                    );

                var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
                await BusyWait.Until(async () => await store.GetFunction(functionId) != null);
                (await rFunc(param)).ShouldBe("TEST");                
                unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
            }
        }
        
        public abstract Task PostponedFunctionIsCompletedByWatchDog();
        protected async Task PostponedFunctionIsCompletedByWatchDog(IFunctionStore store)
        {
            var functionTypeId = nameof(PostponedFunctionIsCompletedByWatchDog).ToFunctionTypeId();
            var unhandledExceptionHandler = new UnhandledExceptionCatcher();
            const string param = "test";
            {
                var function = new PostponedFunctionWrapper(shouldPostpone: true);
                var rFunc = RFunctions
                    .Create(
                        store, 
                        unhandledExceptionHandler.Catch, 
                        crashedCheckFrequency: TimeSpan.Zero,
                        postponedCheckFrequency: TimeSpan.Zero
                    )
                    .Register(
                        functionTypeId,
                        (string s) => function.Method(s),
                        _ => _
                    );

                await SafeTryAsync(async () => await rFunc(param));   
                unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
            }
            {
                var function = new PostponedFunctionWrapper(shouldPostpone: false);
                using var rFunctions = RFunctions.Create(
                    store, 
                    unhandledExceptionHandler.Catch,
                    crashedCheckFrequency: TimeSpan.FromMilliseconds(0),
                    postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
                );

                var rFunc = rFunctions
                    .Register(
                        functionTypeId,
                        (string s) => function.Method(s),
                        _ => _
                    );

                var functionId = new FunctionId(functionTypeId, param.ToFunctionInstanceId());
                await BusyWait.Until(async () => (await store.GetFunction(functionId))!.Status == Status.Succeeded);
                (await rFunc(param)).ShouldBe("TEST");                
                unhandledExceptionHandler.ThrownExceptions.Count.ShouldBe(0);
            }
        }
        
        private class UncaughtExceptionFunctionWrapper
        {
            private readonly bool _shouldThrow;

            public UncaughtExceptionFunctionWrapper(bool shouldThrow) => _shouldThrow = shouldThrow;

            public Task<RResult<string>> Method(string s)
            {
                if (_shouldThrow)
                    throw new Exception();

                return Task.FromResult(Succeed.WithResult(s.ToUpper()));
            }
        }
        
        private class PostponedFunctionWrapper
        {
            private readonly bool _shouldPostpone;

            public PostponedFunctionWrapper(bool shouldPostpone) => _shouldPostpone = shouldPostpone;

            public async Task<RResult<string>> Method(string s)
            {
                await Task.Delay(0);
                
                if (_shouldPostpone)
                    return Postpone.For(TimeSpan.FromMilliseconds(100));

                return s.ToUpper();
            }
        }
    }
}