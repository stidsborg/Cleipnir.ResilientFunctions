using System;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.RFunctionTests;

[TestClass]
public class PreAndPostInvokeTests
{
    private FunctionId FunctionId { get; }
        = new FunctionId("FunctionTypeId", "FunctionInstanceId");
    
    [TestMethod]
    public async Task PreAndPostIsInvokedForAction()
    {
        var rFunctions = new RFunctions(new InMemoryFunctionStore());
        var syncedPreInvoke = new Synced<Metadata<string>>();
        var syncedPostInvoke = new Synced<Tuple<Return, Metadata<string>>>();
        var rAction = rFunctions
            .Action(
                FunctionId.TypeId,
                inner: (string _) => Task.CompletedTask
            )
            .WithPreInvoke(metadata => syncedPreInvoke.Value = metadata)
            .WithPostInvoke(
                (returned, metadata) =>
                {
                    syncedPostInvoke.Value = Tuple.Create(returned, metadata);
                    return returned;
                }
            )
            .Register()
            .Invoke;

        await rAction(FunctionId.InstanceId.ToString(), "hello world");

        var preInvokeMetadata = syncedPreInvoke.Value;
        preInvokeMetadata.ShouldNotBeNull();
        preInvokeMetadata.Param.ShouldBe("hello world");
        preInvokeMetadata.FunctionId.ShouldBe(FunctionId);

        syncedPostInvoke.Value.ShouldNotBeNull();
        var (postInvokeReturn, postInvokeMetadata) = syncedPostInvoke.Value;
        postInvokeReturn.Intent.ShouldBe(Intent.Succeed);
        postInvokeMetadata.Param.ShouldBe("hello world");
        postInvokeMetadata.FunctionId.ShouldBe(FunctionId);
    }
    
    [TestMethod]
    public async Task PreAndPostIsInvokedForActionWithScrapbook()
    {
        var store = new InMemoryFunctionStore();
        var rFunctions = new RFunctions(store);
        var syncedPreInvoke = new Synced<Tuple<Scrapbook, Metadata<string>>>();
        var syncedPostInvoke = new Synced<Tuple<Return, Scrapbook, Metadata<string>>>();
        var rAction = rFunctions
            .ActionWithScrapbook<string, Scrapbook>(
                FunctionId.TypeId,
                inner: (_, _) => Task.CompletedTask
            )
            .WithPreInvoke(
                (scrapbook, metadata) =>
                {
                    syncedPreInvoke.Value = Tuple.Create(scrapbook.Clone(), metadata);
                    scrapbook.Value = "PreInvoked";
                    return Task.CompletedTask;
                })
            .WithPostInvoke((returned, scrapbook, metadata) =>
            {
                syncedPostInvoke.Value = Tuple.Create(returned, scrapbook.Clone(), metadata);
                scrapbook.Value = "PostInvoked";
                return returned.ToTask();
            })
            .Register()
            .Invoke;

        await rAction(FunctionId.InstanceId.ToString(), "hello world");

        syncedPreInvoke.Value.ShouldNotBeNull();
        var (preInvokeScrapbook, preInvokeMetadata) = syncedPreInvoke.Value;
        
        preInvokeScrapbook.ShouldNotBeNull();
        preInvokeScrapbook.Value.ShouldBe("");
        
        preInvokeMetadata.ShouldNotBeNull();
        preInvokeMetadata.Param.ShouldBe("hello world");
        preInvokeMetadata.FunctionId.ShouldBe(FunctionId);

        syncedPostInvoke.Value.ShouldNotBeNull();
        var (postInvokeReturn, postInvokeScrapbook, postInvokeMetadata) = syncedPostInvoke.Value;
        postInvokeScrapbook.ShouldNotBeNull();
        postInvokeScrapbook.Value.ShouldBe("PreInvoked");
        postInvokeReturn.Intent.ShouldBe(Intent.Succeed);
        postInvokeMetadata.Param.ShouldBe("hello world");
        postInvokeMetadata.FunctionId.ShouldBe(FunctionId);

        var storedScrapbook = await store.GetFunction(FunctionId).Map(sf => sf?.Scrapbook);
        storedScrapbook.ShouldNotBeNull();
        var scrapbook = (Scrapbook?) JsonSerializer.Deserialize(
            storedScrapbook.ScrapbookJson!,
            Type.GetType(storedScrapbook.ScrapbookType, throwOnError: true)!
        );
        scrapbook.ShouldNotBeNull();
        scrapbook.Value.ShouldBe("PostInvoked");
    }
    
    [TestMethod]
    public async Task PreAndPostIsInvokedForFunc()
    {
        var rFunctions = new RFunctions(new InMemoryFunctionStore());
        var syncedPreInvoke = new Synced<Metadata<string>>();
        var syncedPostInvoke = new Synced<Tuple<Return<string>, Metadata<string>>>();
        var rFunc = rFunctions.Register<string, string>(
            FunctionId.TypeId,
            inner: param => param.ToUpper().ToTask(),
            preInvoke: metadata =>
            {
                syncedPreInvoke.Value = metadata;
                return Task.CompletedTask;
            },
            postInvoke: (returned, metadata) =>
            {
                syncedPostInvoke.Value = Tuple.Create(returned, metadata);
                return new Return<string>("post invoked").ToTask();
            }).Invoke;

        var returned = await rFunc(FunctionId.InstanceId.ToString(), "hello world");
        returned.ShouldBe("post invoked");

        var preInvokeMetadata = syncedPreInvoke.Value;
        preInvokeMetadata.ShouldNotBeNull();
        preInvokeMetadata.Param.ShouldBe("hello world");
        preInvokeMetadata.FunctionId.ShouldBe(FunctionId);

        syncedPostInvoke.Value.ShouldNotBeNull();
        var (postInvokeReturn, postInvokeMetadata) = syncedPostInvoke.Value;
        postInvokeReturn.Intent.ShouldBe(Intent.Succeed);
        postInvokeReturn.SucceedWithValue.ShouldBe("HELLO WORLD");
        postInvokeMetadata.Param.ShouldBe("hello world");
        postInvokeMetadata.FunctionId.ShouldBe(FunctionId);
    }
    
    [TestMethod]
    public async Task PreAndPostIsInvokedForFuncWithScrapbook()
    {
        var store = new InMemoryFunctionStore();
        var rFunctions = new RFunctions(store);
        var syncedPreInvoke = new Synced<Tuple<Scrapbook, Metadata<string>>>();
        var syncedPostInvoke = new Synced<Tuple<Return<string>, Scrapbook, Metadata<string>>>();
        var rFunc = rFunctions.Register<string, Scrapbook, string>(
            FunctionId.TypeId,
            inner: (param , _) => param.ToUpper().ToTask(),
            preInvoke: (scrapbook, metadata) =>
            {
                syncedPreInvoke.Value = Tuple.Create(scrapbook.Clone(), metadata);
                scrapbook.Value = "PreInvoked";
                return Task.CompletedTask;
            },
            postInvoke: (returned, scrapbook, metadata) =>
            {
                syncedPostInvoke.Value = Tuple.Create(returned, scrapbook.Clone(), metadata);
                scrapbook.Value = "PostInvoked";
                return new Return<string>("post invoked").ToTask();
            }).Invoke;

        var returned = await rFunc(FunctionId.InstanceId.ToString(), "hello world");
        returned.ShouldBe("post invoked");

        syncedPreInvoke.Value.ShouldNotBeNull();
        var (preInvokeScrapbook, preInvokeMetadata) = syncedPreInvoke.Value;
        
        preInvokeScrapbook.ShouldNotBeNull();
        preInvokeScrapbook.Value.ShouldBe("");
        
        preInvokeMetadata.ShouldNotBeNull();
        preInvokeMetadata.Param.ShouldBe("hello world");
        preInvokeMetadata.FunctionId.ShouldBe(FunctionId);

        syncedPostInvoke.Value.ShouldNotBeNull();
        var (postInvokeReturn, postInvokeScrapbook, postInvokeMetadata) = syncedPostInvoke.Value;
        postInvokeScrapbook.ShouldNotBeNull();
        postInvokeScrapbook.Value.ShouldBe("PreInvoked");
        postInvokeReturn.Intent.ShouldBe(Intent.Succeed);
        postInvokeReturn.SucceedWithValue.ShouldBe("HELLO WORLD");
        postInvokeMetadata.Param.ShouldBe("hello world");
        postInvokeMetadata.FunctionId.ShouldBe(FunctionId);

        var storedScrapbook = await store.GetFunction(FunctionId).Map(sf => sf?.Scrapbook);
        storedScrapbook.ShouldNotBeNull();
        var scrapbook = (Scrapbook?) JsonSerializer.Deserialize(
            storedScrapbook.ScrapbookJson!,
            Type.GetType(storedScrapbook.ScrapbookType, throwOnError: true)!
        );
        scrapbook.ShouldNotBeNull();
        scrapbook.Value.ShouldBe("PostInvoked");
    }

    private class Scrapbook : RScrapbook
    {
        public string Value { get; set; } = "";

        public Scrapbook Clone() => new() {Value = Value};
    }
}