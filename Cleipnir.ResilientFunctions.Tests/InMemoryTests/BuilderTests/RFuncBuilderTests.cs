using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.BuilderTests;

[TestClass]
public class RFuncBuilderTests
{
    private readonly FunctionTypeId _functionTypeId = new FunctionTypeId("FunctionTypeId");
    private const string FunctionInstanceId = "FunctionInstanceId";
    
    [TestMethod]
    public async Task ConstructedFuncInvokeCanBeCreatedAndInvoked()
    {
        using var rFunctions = CreateRFunctions();
        var rFunc = rFunctions
            .Func<string, string>(
                _functionTypeId,
                InnerFunc
            )
            .Register()
            .Invoke;

        var result = await rFunc(FunctionInstanceId, "hello world");
        result.ShouldBe("HELLO WORLD");
    }
    
    [TestMethod]
    public async Task ConstructedFuncWithPreAndPostInvokeCanBeCreatedAndInvoked()
    {
        using var rFunctions = CreateRFunctions();
        var preInvokeFlag = new SyncedFlag();
        var postInvokeFlag = new SyncedFlag();
        var rFunc = rFunctions
            .Func<string, string>(
                _functionTypeId,
                InnerFunc
            )
            .WithPreInvoke(_ => preInvokeFlag.Raise())
            .WithPostInvoke((returned, _) => { postInvokeFlag.Raise(); return returned; })
            .Register()
            .Invoke;

        var result = await rFunc(FunctionInstanceId, "hello world");
        result.ShouldBe("HELLO WORLD");
        preInvokeFlag.Position.ShouldBe(FlagPosition.Raised);
        postInvokeFlag.Position.ShouldBe(FlagPosition.Raised);
    }
    
    [TestMethod]
    public async Task ConstructedFuncWithCustomSerializerCanBeCreatedAndInvoked()
    {
        using var rFunctions = CreateRFunctions();
        var serializer = new Serializer();
        var rFunc = rFunctions
            .Func<string, string>(
                _functionTypeId,
                InnerFunc
            )
            .WithSerializer(serializer)
            .Register()
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

        public object DeserializeParameter(string json, string type) => Default.DeserializeParameter(json, type);

        public string SerializeScrapbook(RScrapbook scrapbook) => Default.SerializeScrapbook(scrapbook);

        public RScrapbook DeserializeScrapbook(string? json, string type) => Default.DeserializeScrapbook(json, type);

        public string SerializeError(RError error) => Default.SerializeError(error);

        public RError DeserializeError(string json) => Default.DeserializeError(json);

        public string SerializeResult(object result) => Default.SerializeResult(result);

        public object DeserializeResult(string json, string type) => Default.DeserializeResult(json, type);
    }
}