using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RegistrationTests;

[TestClass]
public class RActionWithScrapbookRegistrationTests
{
    private readonly FunctionTypeId _functionTypeId = new FunctionTypeId("FunctionTypeId");
    private const string FunctionInstanceId = "FunctionInstanceId";
    
    [TestMethod]
    public async Task ConstructedFuncInvokeCanBeCreatedAndInvoked()
    {
        using var rFunctions = CreateRFunctions();
        var rAction = rFunctions
            .RegisterAction<string, Scrapbook>(
                _functionTypeId,
                InnerAction
            )
            .Invoke;

        await rAction(FunctionInstanceId, "hello world");
    }
    
    [TestMethod]
    public async Task ConstructedFuncWithCustomSerializerCanBeCreatedAndInvoked()
    {
        using var rFunctions = CreateRFunctions();
        var serializer = new Serializer();
        var rAction = rFunctions
            .RegisterAction<string, Scrapbook>(
                _functionTypeId,
                InnerAction,
                version: 0,
                new Settings(serializer: serializer)
            )
            .Invoke;

        await rAction(FunctionInstanceId, "hello world");
        serializer.Invoked.ShouldBeTrue();
    }

    private async Task InnerAction(string param, Scrapbook scrapbook) => await Task.CompletedTask;
    private RFunctions CreateRFunctions() => new(new InMemoryFunctionStore());

    private class Serializer : ISerializer
    {
        public bool Invoked { get; set; }
        private ISerializer Default { get; } = DefaultSerializer.Instance;

        public StoredParameter SerializeParameter<TParam>(TParam parameter) where TParam : notnull
        {
            Invoked = true;
            return Default.SerializeParameter(parameter);
        }

        public TParam DeserializeParameter<TParam>(string json, string type) where TParam : notnull
            => Default.DeserializeParameter<TParam>(json, type);

        public StoredScrapbook SerializeScrapbook<TScrapbook>(TScrapbook scrapbook) where TScrapbook : RScrapbook
            => Default.SerializeScrapbook(scrapbook);
        public TScrapbook DeserializeScrapbook<TScrapbook>(string json, string type) where TScrapbook : RScrapbook 
            => Default.DeserializeScrapbook<TScrapbook>(json, type);

        public StoredException SerializeException(Exception exception)
            => Default.SerializeException(exception);
        public PreviouslyThrownException DeserializeException(StoredException storedException)
            => Default.DeserializeException(storedException);

        public StoredResult SerializeResult<TResult>(TResult result) => Default.SerializeResult(result);
        public TResult DeserializeResult<TResult>(string json, string type) 
            => Default.DeserializeResult<TResult>(json, type);
    }
    
    private class Scrapbook : RScrapbook {}
}