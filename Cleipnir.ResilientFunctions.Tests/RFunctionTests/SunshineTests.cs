using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.RFunctionTests
{
    public abstract class SunshineTests
    {
        public abstract Task SunshineScenarioFunc();
        public async Task SunshineScenarioFunc(IFunctionStore store)
        {
            var functionTypeId = nameof(SunshineScenarioFunc).ToFunctionTypeId();
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

            var rResult = await rFunc("hello");
            var result = rResult.SuccessResult;
            result.ShouldBe("HELLO");
            
            var storedFunction = await store.GetFunction(
                new FunctionId(
                    functionTypeId, 
                    "hello".ToFunctionInstanceId()
                )
            );
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            storedFunction.Result.ShouldNotBeNull();
            var storedResult = storedFunction.Result.Deserialize().ToString();
            storedResult.ShouldBe("HELLO");
            
            unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
        }
        
        public abstract Task SunshineScenarioFuncWithScrapbook();
        public async Task SunshineScenarioFuncWithScrapbook(IFunctionStore store)
        {
            var functionTypeId = nameof(SunshineScenarioFuncWithScrapbook).ToFunctionTypeId();
            async Task<RResult<string>> ToUpper(string s, Scrapbook scrapbook)
            {
                await scrapbook.Save();
                return s.ToUpper();
            }

            var unhandledExceptionHandler = new UnhandledExceptionCatcher();

            var rFunctions = RFunctions.Create(store, unhandledExceptionHandler.Catch);

            var rFunc = rFunctions
                .Register(
                    functionTypeId,
                    (string s, Scrapbook scrapbook) => ToUpper(s, scrapbook),
                    _ => _
                );

            var rResult = await rFunc("hello");
            var result = rResult.SuccessResult;
            result.ShouldBe("HELLO");
            
            var storedFunction = await store.GetFunction(
                new FunctionId(
                    functionTypeId, 
                    "hello".ToFunctionInstanceId()
                )
            );
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            storedFunction.Result.ShouldNotBeNull();
            var storedResult = storedFunction.Result.Deserialize().ToString();
            storedResult.ShouldBe("HELLO");
            
            unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
        }
        
        public abstract Task SunshineScenarioAction();
        public async Task SunshineScenarioAction(IFunctionStore store)
        {
            var functionTypeId = nameof(SunshineScenarioAction).ToFunctionTypeId();
            async Task<RResult> ToUpper(string _)
            {
                await Task.Delay(10);
                return RResult.Success;
            }

            var unhandledExceptionHandler = new UnhandledExceptionCatcher();

            var rFunctions = RFunctions.Create(store, unhandledExceptionHandler.Catch);

            var rFunc = rFunctions
                .Register(
                    functionTypeId,
                    (string s) => ToUpper(s),
                    _ => _
                );

            var rResult = await rFunc("hello");
            rResult.Succeeded.ShouldBeTrue();

            var storedFunction = await store.GetFunction(
                new FunctionId(
                    functionTypeId, 
                    "hello".ToFunctionInstanceId()
                )
            );
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
        }
        
        public abstract Task SunshineScenarioActionWithScrapbook();
        public async Task SunshineScenarioActionWithScrapbook(IFunctionStore store)
        {
            var functionTypeId = nameof(SunshineScenarioActionWithScrapbook).ToFunctionTypeId();
            async Task<RResult> ToUpper(string _, Scrapbook scrapbook)
            {
                await scrapbook.Save();
                return RResult.Success;
            }

            var unhandledExceptionHandler = new UnhandledExceptionCatcher();

            var rFunctions = RFunctions.Create(store, unhandledExceptionHandler.Catch);

            var rFunc = rFunctions
                .Register(
                    functionTypeId,
                    (string s, Scrapbook scrapbook) => ToUpper(s, scrapbook),
                    _ => _
                );

            var rResult = await rFunc("hello");
            rResult.Succeeded.ShouldBeTrue();

            var storedFunction = await store.GetFunction(
                new FunctionId(
                    functionTypeId, 
                    "hello".ToFunctionInstanceId()
                )
            );
            storedFunction.ShouldNotBeNull();
            storedFunction.Status.ShouldBe(Status.Succeeded);
            unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
        }

        private class Scrapbook : RScrapbook {}
    }
}