using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates
{
    public abstract class RFunctionScrapbookTests
    {
        private readonly DefaultSerializer _serializer = DefaultSerializer.Instance;
        public abstract Task SunshineScenario();
        public async Task SunshineScenario(IFunctionStore store)
        {
            var functionTypeId = nameof(SunshineScenario).ToFunctionTypeId();
            async Task<string> ToUpper(string s, Scrapbook scrapbook)
            {
                var toReturn = s.ToUpper();
                scrapbook.Scrap = toReturn;
                await scrapbook.Save();
                return s.ToUpper();
            }

            var unhandledExceptionHandler = new UnhandledExceptionCatcher();

            using var rFunctions = new RFunctions(store, unhandledExceptionHandler.Catch);

            var rFunc = rFunctions
                .FuncWithScrapbook(
                    functionTypeId,
                    (string s, Scrapbook scrapbook) => ToUpper(s, scrapbook)
                ).Register().Invoke;

            var result = await rFunc("hello", "hello");
            result.ShouldBe("HELLO");
            
            var storedFunction = await store.GetFunction(
                new FunctionId(
                    functionTypeId, 
                    "hello".ToFunctionInstanceId()
                )
            );
            storedFunction.ShouldNotBeNull();
            storedFunction.Result.ShouldNotBeNull();
            var storedResult = storedFunction.Result.Deserialize(_serializer);
            storedResult.ShouldBe("HELLO");
            storedFunction.Scrapbook.ShouldNotBeNull();
            var scrapbook = (Scrapbook) storedFunction.Scrapbook.Deserialize(_serializer);
            scrapbook.Scrap.ShouldBe("HELLO");
            
            unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
        }

        private class Scrapbook : RScrapbook
        {
            public string Scrap { get; set; } = "";
        }
    }
}