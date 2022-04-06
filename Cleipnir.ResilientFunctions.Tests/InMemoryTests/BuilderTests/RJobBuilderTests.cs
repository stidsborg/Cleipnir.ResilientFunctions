using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.BuilderTests;

[TestClass]
public class RJobBuilderTests
{
    private FunctionId FunctionId { get; } = new("Job", JobId);
    private const string JobId = "TestJob";
    
    [TestMethod]
    public async Task ConstructedActionJobIsCompletedSuccessfully()
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
        await rFunctions
            .RegisterJob<Scrapbook>(JobId)
            .WithInner(scrapbook => scrapbook.Value = "invoked")
            .Create()
            .Start();

        await BusyWait.Until(
            () => store
                .GetFunction(FunctionId)
                .Map(sf => sf?.Status == Status.Succeeded)
        );
        var sf = await store.GetFunction(FunctionId);
        var scrapbook = DefaultSerializer.Instance.DeserializeScrapbook(
            sf!.Scrapbook!.ScrapbookJson,
            sf.Scrapbook!.ScrapbookType
        ).CastTo<Scrapbook>();
        
        scrapbook.Value.ShouldBe("invoked");
    }
    
    [TestMethod]
    public async Task ConstructedAsyncJobIsCompletedSuccessfully()
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
        await rFunctions
            .RegisterJob<Scrapbook>(JobId)
            .WithInner(scrapbook =>
            {
                scrapbook.Value = "invoked";
                return Task.CompletedTask;
            })
            .Create()
            .Start();

        await BusyWait.Until(
            () => store
                .GetFunction(FunctionId)
                .Map(sf => sf?.Status == Status.Succeeded)
        );
        var sf = await store.GetFunction(FunctionId);
        var scrapbook = DefaultSerializer.Instance.DeserializeScrapbook(
            sf!.Scrapbook!.ScrapbookJson,
            sf.Scrapbook!.ScrapbookType
        ).CastTo<Scrapbook>();
        
        scrapbook.Value.ShouldBe("invoked");
    }
    
    [TestMethod]
    public async Task ConstructedJobIsCompletedSuccessfully()
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
        await rFunctions
            .RegisterJob<Scrapbook>(JobId)
            .WithInner(scrapbook =>
            {
                scrapbook.Value = "invoked";
                return Result.Succeed.ToTask();
            })
            .Create()
            .Start();

        await BusyWait.Until(
            () => store
                .GetFunction(FunctionId)
                .Map(sf => sf?.Status == Status.Succeeded)
        );
        var sf = await store.GetFunction(FunctionId);
        var scrapbook = DefaultSerializer.Instance.DeserializeScrapbook(
            sf!.Scrapbook!.ScrapbookJson,
            sf.Scrapbook!.ScrapbookType
        ).CastTo<Scrapbook>();
        
        scrapbook.Value.ShouldBe("invoked");
    }
    
    [TestMethod]
    public async Task ConstructedJobWithReturnIsCompletedSuccessfully()
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
        await rFunctions
            .RegisterJob<Scrapbook>(JobId)
            .WithInner(scrapbook =>
            {
                scrapbook.Value = "invoked";
                return Result.Succeed;
            })
            .Create()
            .Start();

        await BusyWait.Until(
            () => store
                .GetFunction(FunctionId)
                .Map(sf => sf?.Status == Status.Succeeded)
        );
        var sf = await store.GetFunction(FunctionId);
        var scrapbook = DefaultSerializer.Instance.DeserializeScrapbook(
            sf!.Scrapbook!.ScrapbookJson,
            sf.Scrapbook!.ScrapbookType
        ).CastTo<Scrapbook>();
        
        scrapbook.Value.ShouldBe("invoked");
    }
    
    [TestMethod]
    public async Task ConstructedFuncWithPreAndPostInvokeCanBeCreatedAndInvoked()
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
        
        var preInvokeFlag = new SyncedFlag();
        var postInvokeFlag = new SyncedFlag();
        var serializer = new Serializer();
        var rJob = rFunctions
            .RegisterJob<Scrapbook>(JobId)
            .WithInner(_ => { })
            .WithPreInvoke(_ => preInvokeFlag.Raise())
            .WithPostInvoke((result, _) =>
            {
                postInvokeFlag.Raise();
                return result;
            })
            .WithSerializer(serializer)
            .Register()
            .Start;

        await rJob();

        await preInvokeFlag.WaitForRaised().WithTimeout(1_000);
        await postInvokeFlag.WaitForRaised().WithTimeout(1_000);
        serializer.Invoked.ShouldBeTrue();
    }
    
    private class Serializer : ISerializer
    {
        public bool Invoked { get; private set; }
        private static ISerializer Default => DefaultSerializer.Instance;

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

    private class Scrapbook : RScrapbook
    {
        public string Value { get; set; } = "";
    }
}