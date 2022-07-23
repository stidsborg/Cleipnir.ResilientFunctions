﻿using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RegistrationTests;

[TestClass]
public class RJobRegistrationTests
{
    private FunctionId FunctionId { get; } = new(JobId, "Job");
    private const string JobId = "TestJob";
    
    [TestMethod]
    public async Task ConstructedActionJobIsCompletedSuccessfully()
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
        await rFunctions
            .RegisterJob<Scrapbook>(JobId, scrapbook => 
                scrapbook.Value = "invoked")
            .Start();

        await BusyWait.Until(
            () => store
                .GetFunction(FunctionId)
                .Map(sf => sf?.Status == Status.Succeeded)
        );
        var sf = await store.GetFunction(FunctionId);
        var scrapbook = DefaultSerializer.Instance.DeserializeScrapbook<Scrapbook>(
            sf!.Scrapbook!.ScrapbookJson,
            sf.Scrapbook!.ScrapbookType
        );
        
        scrapbook.Value.ShouldBe("invoked");
    }
    
    [TestMethod]
    public async Task ConstructedAsyncJobIsCompletedSuccessfully()
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
        await rFunctions
            .RegisterJob<Scrapbook>(
                JobId,
                scrapbook =>
                {
                    scrapbook.Value = "invoked";
                    return Task.CompletedTask;
                }
            ).Start();

        await BusyWait.Until(
            () => store
                .GetFunction(FunctionId)
                .Map(sf => sf?.Status == Status.Succeeded)
        );
        var sf = await store.GetFunction(FunctionId);
        var scrapbook = DefaultSerializer.Instance.DeserializeScrapbook<Scrapbook>(
            sf!.Scrapbook!.ScrapbookJson,
            sf.Scrapbook!.ScrapbookType
        );
        
        scrapbook.Value.ShouldBe("invoked");
    }
    
    [TestMethod]
    public async Task ConstructedJobIsCompletedSuccessfully()
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
        await rFunctions
            .RegisterJob<Scrapbook>(
                JobId,
                scrapbook =>
                {
                    scrapbook.Value = "invoked";
                    return Result.Succeed.ToTask();
                })
            .Start();

        await BusyWait.Until(
            () => store
                .GetFunction(FunctionId)
                .Map(sf => sf?.Status == Status.Succeeded)
        );
        var sf = await store.GetFunction(FunctionId);
        var scrapbook = DefaultSerializer.Instance.DeserializeScrapbook<Scrapbook>(
            sf!.Scrapbook!.ScrapbookJson,
            sf.Scrapbook!.ScrapbookType
        );
        
        scrapbook.Value.ShouldBe("invoked");
    }
    
    [TestMethod]
    public async Task ConstructedJobWithReturnIsCompletedSuccessfully()
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
        await rFunctions
            .RegisterJob<Scrapbook>(
                JobId,
                scrapbook =>
                {
                    scrapbook.Value = "invoked";
                    return Result.Succeed;
                }
            ).Start();

        await BusyWait.Until(
            () => store
                .GetFunction(FunctionId)
                .Map(sf => sf?.Status == Status.Succeeded)
        );
        var sf = await store.GetFunction(FunctionId);
        var scrapbook = DefaultSerializer.Instance.DeserializeScrapbook<Scrapbook>(
            sf!.Scrapbook!.ScrapbookJson,
            sf.Scrapbook!.ScrapbookType
        );
        
        scrapbook.Value.ShouldBe("invoked");
    }
    
    [TestMethod]
    public async Task ConstructedFuncWithSerializerCreatedAndInvoked()
    {
        var store = new InMemoryFunctionStore();
        using var rFunctions = new RFunctions(store);
        
        var flag = new SyncedFlag();
        var serializer = new Serializer();
        var rJob = rFunctions
            .RegisterJob<Scrapbook>(
                JobId,
                _ => flag.Raise(),
                new Settings(Serializer: serializer)
            ).Start;

        await rJob();

        await flag.WaitForRaised().WithTimeout(1_000);
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
        public TParam DeserializeParameter<TParam>(string json, string type) 
            => Default.DeserializeParameter<TParam>(json, type);

        public string SerializeScrapbook(RScrapbook scrapbook) => Default.SerializeScrapbook(scrapbook);
        public TScrapbook DeserializeScrapbook<TScrapbook>(string? json, string type) where TScrapbook : RScrapbook
            => Default.DeserializeScrapbook<TScrapbook>(json, type);

        public string SerializeError(RError error) => Default.SerializeError(error);
        public RError DeserializeError(string json) => Default.DeserializeError(json);

        public string SerializeResult(object result) => Default.SerializeResult(result);
        public TResult DeserializeResult<TResult>(string json, string type) 
            => Default.DeserializeResult<TResult>(json, type);
    }

    private class Scrapbook : RScrapbook
    {
        public string Value { get; set; } = "";
    }
}