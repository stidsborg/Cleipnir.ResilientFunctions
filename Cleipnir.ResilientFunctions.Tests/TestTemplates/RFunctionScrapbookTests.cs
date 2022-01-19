using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates
{
    public abstract class RFunctionScrapbookTests
    {
        public abstract Task SunshineScenario();
        public async Task SunshineScenario(IFunctionStore store)
        {
            var functionTypeId = nameof(SunshineScenario).ToFunctionTypeId();
            async Task<RResult<string>> ToUpper(string s, Scrapbook scrapbook)
            {
                var toReturn = s.ToUpper();
                scrapbook.Scrap = toReturn;
                await scrapbook.Save();
                return s.ToUpper();
            }

            var unhandledExceptionHandler = new UnhandledExceptionCatcher();

            using var rFunctions = RFunctions.Create(store, unhandledExceptionHandler.Catch);

            var rFunc = rFunctions
                .Register(
                    functionTypeId,
                    (string s, Scrapbook scrapbook) => ToUpper(s, scrapbook),
                    _ => _
                );

            var result = await rFunc("hello").EnsureSuccess();
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
            storedFunction.Scrapbook.ShouldNotBeNull();
            var scrapbook = (Scrapbook) storedFunction.Scrapbook.Deserialize();
            scrapbook.Scrap.ShouldBe("HELLO");
            
            unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
        }

        private class Scrapbook : RScrapbook
        {
            public string Scrap { get; set; } = "";
        }
    }
}