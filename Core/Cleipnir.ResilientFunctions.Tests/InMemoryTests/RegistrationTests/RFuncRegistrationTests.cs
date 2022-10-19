using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RegistrationTests;

[TestClass]
public class RFuncRegistrationTests
{
    private readonly FunctionTypeId _functionTypeId = new FunctionTypeId("FunctionTypeId");
    private const string FunctionInstanceId = "FunctionInstanceId";
    
    [TestMethod]
    public async Task ConstructedFuncInvokeCanBeCreatedAndInvoked()
    {
        using var rFunctions = CreateRFunctions();
        var rFunc = rFunctions
            .RegisterFunc<string, string>(
                _functionTypeId,
                InnerFunc
            )
            .Invoke;

        var result = await rFunc(FunctionInstanceId, "hello world");
        result.ShouldBe("HELLO WORLD");
    }

    [TestMethod]
    public async Task ConstructedFuncWithCustomSerializerCanBeCreatedAndInvoked()
    {
        using var rFunctions = CreateRFunctions();
        var serializer = new Serializer();
        var rFunc = rFunctions
            .RegisterFunc<string, string>(
                _functionTypeId,
                InnerFunc,
                version: 0,
                new Settings(Serializer: serializer)
            )
            .Invoke;

        var result = await rFunc(FunctionInstanceId, "hello world");
        result.ShouldBe("HELLO WORLD");
        serializer.Invoked.ShouldBeTrue();
    }

    private async Task<string> InnerFunc(string param)
    {
        await Task.CompletedTask;
        return param.ToUpper();
    }
    private RFunctions CreateRFunctions() => new(new InMemoryFunctionStore());

    private class Serializer : ISerializer
    {
        public bool Invoked { get; set; }
        private ISerializer Default { get; } = DefaultSerializer.Instance;
        
        public string SerializeParameter(object parameter)
        {
            Invoked = true;
            return Default.SerializeParameter(parameter);
        }
        public TParam DeserializeParameter<TParam>(string json, string type) 
            => Default.DeserializeParameter<TParam>(json, type);

        public string SerializeScrapbook(RScrapbook scrapbook) => Default.SerializeScrapbook(scrapbook);
        public TScrapbook DeserializeScrapbook<TScrapbook>(string json, string type) where TScrapbook : RScrapbook 
            => Default.DeserializeScrapbook<TScrapbook>(json, type);

        public string SerializeError(RError error) => Default.SerializeError(error);
        public RError DeserializeError(string json) => Default.DeserializeError(json);

        public string SerializeResult(object result) => Default.SerializeResult(result);
        public TResult DeserializeResult<TResult>(string json, string type) 
            => Default.DeserializeResult<TResult>(json, type);
    }
}